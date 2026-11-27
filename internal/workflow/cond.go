package workflow

import (
	"fmt"
	"strings"
	"unicode"
)

type condTokenKind int

const (
	tokEOF condTokenKind = iota
	tokLParen
	tokRParen
	tokNot
	tokAnd
	tokOr
	tokEq
	tokNeq
	tokString
	tokIdent
)

type condToken struct {
	kind   condTokenKind
	val    string
	quoted bool
}

// ValidateCondition parses a task `if:` expression and rejects syntax errors.
func ValidateCondition(expr string) error {
	if strings.TrimSpace(expr) == "" {
		return nil
	}
	_, err := evalConditionAST(expr, nil, false)
	return err
}

// EvalCondition evaluates a task `if:` expression against task environment variables.
// An empty expression always evaluates to true.
func EvalCondition(expr string, envVars map[string]string) (bool, error) {
	if strings.TrimSpace(expr) == "" {
		return true, nil
	}
	return evalConditionAST(expr, envVars, true)
}

func evalConditionAST(expr string, envVars map[string]string, evaluate bool) (bool, error) {
	tokens, err := tokenizeCondition(expr)
	if err != nil {
		return false, err
	}
	p := &condParser{
		tokens:   tokens,
		envVars:  envVars,
		evaluate: evaluate,
	}
	res, err := p.parseOr()
	if err != nil {
		return false, err
	}
	if p.peek().kind != tokEOF {
		return false, fmt.Errorf("unexpected token %q in condition", p.peek().val)
	}
	return res, nil
}

func tokenizeCondition(input string) ([]condToken, error) {
	var tokens []condToken
	i := 0
	for i < len(input) {
		ch := input[i]
		if unicode.IsSpace(rune(ch)) {
			i++
			continue
		}
		if i+1 < len(input) {
			two := input[i : i+2]
			switch two {
			case "&&":
				tokens = append(tokens, condToken{kind: tokAnd, val: two})
				i += 2
				continue
			case "||":
				tokens = append(tokens, condToken{kind: tokOr, val: two})
				i += 2
				continue
			case "==":
				tokens = append(tokens, condToken{kind: tokEq, val: two})
				i += 2
				continue
			case "!=":
				tokens = append(tokens, condToken{kind: tokNeq, val: two})
				i += 2
				continue
			}
		}
		switch ch {
		case '(':
			tokens = append(tokens, condToken{kind: tokLParen, val: "("})
			i++
		case ')':
			tokens = append(tokens, condToken{kind: tokRParen, val: ")"})
			i++
		case '!':
			tokens = append(tokens, condToken{kind: tokNot, val: "!"})
			i++
		case '\'', '"':
			quote := ch
			i++
			var sb strings.Builder
			closed := false
			for i < len(input) {
				if input[i] == '\\' && i+1 < len(input) {
					i++
					sb.WriteByte(input[i])
					i++
					continue
				}
				if input[i] == quote {
					i++
					closed = true
					break
				}
				sb.WriteByte(input[i])
				i++
			}
			if !closed {
				return nil, fmt.Errorf("unterminated string literal in condition")
			}
			tokens = append(tokens, condToken{kind: tokString, val: sb.String(), quoted: true})
		default:
			start := i
			for i < len(input) {
				c := input[i]
				if unicode.IsSpace(rune(c)) || c == '(' || c == ')' || c == '!' || c == '&' || c == '|' || c == '=' || c == '\'' || c == '"' {
					break
				}
				i++
			}
			if i == start {
				return nil, fmt.Errorf("unexpected character %q in condition", string(ch))
			}
			tokens = append(tokens, condToken{kind: tokIdent, val: input[start:i]})
		}
	}
	tokens = append(tokens, condToken{kind: tokEOF})
	return tokens, nil
}

type condParser struct {
	tokens   []condToken
	pos      int
	envVars  map[string]string
	evaluate bool
}

func (p *condParser) peek() condToken {
	if p.pos >= len(p.tokens) {
		return condToken{kind: tokEOF}
	}
	return p.tokens[p.pos]
}

func (p *condParser) next() condToken {
	tok := p.peek()
	if p.pos < len(p.tokens) {
		p.pos++
	}
	return tok
}

func (p *condParser) parseOr() (bool, error) {
	left, err := p.parseAnd()
	if err != nil {
		return false, err
	}
	for p.peek().kind == tokOr {
		p.next()
		right, err := p.parseAnd()
		if err != nil {
			return false, err
		}
		left = left || right
	}
	return left, nil
}

func (p *condParser) parseAnd() (bool, error) {
	left, err := p.parseUnary()
	if err != nil {
		return false, err
	}
	for p.peek().kind == tokAnd {
		p.next()
		right, err := p.parseUnary()
		if err != nil {
			return false, err
		}
		left = left && right
	}
	return left, nil
}

func (p *condParser) parseUnary() (bool, error) {
	if p.peek().kind == tokNot {
		p.next()
		v, err := p.parseUnary()
		if err != nil {
			return false, err
		}
		return !v, nil
	}
	if p.peek().kind == tokLParen {
		p.next()
		v, err := p.parseOr()
		if err != nil {
			return false, err
		}
		if p.peek().kind != tokRParen {
			return false, fmt.Errorf("missing closing ')' in condition")
		}
		p.next()
		return v, nil
	}
	return p.parsePrimary()
}

func (p *condParser) parsePrimary() (bool, error) {
	lhsTok := p.peek()
	if lhsTok.kind != tokString && lhsTok.kind != tokIdent {
		return false, fmt.Errorf("expected operand near %q in condition", lhsTok.val)
	}
	p.next()

	if op := p.peek().kind; op == tokEq || op == tokNeq {
		p.next()
		rhsTok := p.peek()
		if rhsTok.kind != tokString && rhsTok.kind != tokIdent {
			return false, fmt.Errorf("expected right-hand operand after comparison operator")
		}
		p.next()
		if !p.evaluate {
			return false, nil
		}
		lhs := resolveCondOperand(lhsTok, p.envVars)
		rhs := resolveCondOperand(rhsTok, p.envVars)
		if op == tokEq {
			return lhs == rhs, nil
		}
		return lhs != rhs, nil
	}

	if !p.evaluate {
		return false, nil
	}
	val := resolveCondOperand(lhsTok, p.envVars)
	return isTruthyValue(val), nil
}

func resolveCondOperand(tok condToken, envVars map[string]string) string {
	if tok.quoted {
		return expandCondVars(tok.val, envVars)
	}
	raw := tok.val
	if strings.Contains(raw, "${") {
		return expandCondVars(raw, envVars)
	}
	if strings.EqualFold(raw, "true") {
		return "true"
	}
	if strings.EqualFold(raw, "false") {
		return "false"
	}
	if isNumericLiteral(raw) {
		return raw
	}
	if isIdentifier(raw) && envVars != nil {
		return envVars[raw]
	}
	if isIdentifier(raw) {
		return ""
	}
	return raw
}

func expandCondVars(s string, envVars map[string]string) string {
	if !strings.Contains(s, "${") {
		return s
	}
	var out strings.Builder
	for i := 0; i < len(s); {
		if i+2 < len(s) && s[i] == '$' && s[i+1] == '{' {
			if end := strings.IndexByte(s[i+2:], '}'); end > 0 {
				name := s[i+2 : i+2+end]
				if envVars != nil {
					out.WriteString(envVars[name])
				}
				i = i + 2 + end + 1
				continue
			}
		}
		out.WriteByte(s[i])
		i++
	}
	return out.String()
}

func isTruthyValue(val string) bool {
	trimmed := strings.TrimSpace(val)
	if trimmed == "" {
		return false
	}
	switch strings.ToLower(trimmed) {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

func isNumericLiteral(s string) bool {
	if s == "" {
		return false
	}
	start := 0
	if s[0] == '-' || s[0] == '+' {
		if len(s) == 1 {
			return false
		}
		start = 1
	}
	for i := start; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func isIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		letter := (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_'
		digit := ch >= '0' && ch <= '9'
		if i == 0 && !letter {
			return false
		}
		if !letter && !digit {
			return false
		}
	}
	return true
}
