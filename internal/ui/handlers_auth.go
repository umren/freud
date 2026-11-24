package ui

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"freud/internal/auth"
	"freud/internal/db"
)

const sessionCookieName = "freud_session"

type authCtxKey string

const currentUserCtxKey authCtxKey = "freud_current_user"

type authPageViewData struct {
	Title    string
	Username string
	Error    string
}

type userRowView struct {
	Username  string
	IsAdmin   bool
	CreatedAt string
}

type auditRowView struct {
	CreatedAt  string
	Username   string
	Action     string
	ActionSlug string
	Target     string
	Source     string
	Details    string
	SearchKey  string
}

type auditViewData struct {
	basePageData
	Users        []userRowView
	AuditLogs    []auditRowView
	ActionFilter string
	FlashMsg     string
	FlashErr     string
}

func userFromRequest(r *http.Request) *db.UserRecord {
	if r == nil {
		return nil
	}
	if u, ok := r.Context().Value(currentUserCtxKey).(*db.UserRecord); ok {
		return u
	}
	return nil
}

func (s *Server) actorName(r *http.Request) string {
	if u := userFromRequest(r); u != nil && u.Username != "" {
		return u.Username
	}
	return "local"
}

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.cfg.EnableAuth {
			next.ServeHTTP(w, r)
			return
		}

		p := r.URL.Path
		if strings.HasPrefix(p, "/static/") || p == "/setup" || p == "/login" {
			next.ServeHTTP(w, r)
			return
		}

		hasAdmin, err := s.store.HasAdmin(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !hasAdmin {
			redirectOrHTMX(w, r, "/setup")
			return
		}

		cookie, err := r.Cookie(sessionCookieName)
		if err != nil || strings.TrimSpace(cookie.Value) == "" {
			redirectOrHTMX(w, r, "/login")
			return
		}

		u, err := s.store.GetSessionUser(r.Context(), cookie.Value)
		if err != nil {
			clearSessionCookie(w)
			redirectOrHTMX(w, r, "/login")
			return
		}

		ctx := context.WithValue(r.Context(), currentUserCtxKey, u)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func redirectOrHTMX(w http.ResponseWriter, r *http.Request, target string) {
	if r.Header.Get("HX-Request") == "true" {
		w.Header().Set("HX-Redirect", target)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}

func setSessionCookie(w http.ResponseWriter, token string, expires time.Time) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		Expires:  expires,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
}

func clearSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
}

func (s *Server) renderAuthPage(w http.ResponseWriter, tmplName string, data authPageViewData) {
	tmpl, ok := s.pages[tmplName]
	if !ok {
		http.Error(w, "template not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.ExecuteTemplate(w, "auth_layout", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) handleSetupGet(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.EnableAuth {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	hasAdmin, err := s.store.HasAdmin(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if hasAdmin {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	s.renderAuthPage(w, "setup", authPageViewData{Title: "Admin Setup", Username: "admin"})
}

func (s *Server) handleSetupPost(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.EnableAuth {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	hasAdmin, err := s.store.HasAdmin(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if hasAdmin {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}

	usernameRaw := r.FormValue("username")
	if strings.TrimSpace(usernameRaw) == "" {
		usernameRaw = "admin"
	}
	username, err := auth.ValidateUsername(usernameRaw)
	if err != nil {
		s.renderAuthPage(w, "setup", authPageViewData{Title: "Admin Setup", Username: usernameRaw, Error: err.Error()})
		return
	}

	pass := r.FormValue("password")
	confirm := r.FormValue("confirm_password")
	if pass != confirm {
		s.renderAuthPage(w, "setup", authPageViewData{Title: "Admin Setup", Username: username, Error: "Passwords do not match"})
		return
	}
	hash, err := auth.HashPassword(pass)
	if err != nil {
		s.renderAuthPage(w, "setup", authPageViewData{Title: "Admin Setup", Username: username, Error: err.Error()})
		return
	}

	if _, err := s.store.CreateUser(r.Context(), username, hash, true); err != nil {
		s.renderAuthPage(w, "setup", authPageViewData{Title: "Admin Setup", Username: username, Error: err.Error()})
		return
	}

	tok, err := auth.NewSessionToken()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	exp := auth.SessionExpiry()
	if err := s.store.CreateSession(r.Context(), tok, username, exp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	setSessionCookie(w, tok, exp)

	_ = s.store.RecordAudit(r.Context(), username, "SETUP_ADMIN", username, "dream", "Initial administrator account created")
	_ = s.store.RecordAudit(r.Context(), username, "LOGIN", "dream", "dream", "Signed in after setup")
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Server) handleLoginGet(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.EnableAuth {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	hasAdmin, err := s.store.HasAdmin(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !hasAdmin {
		http.Redirect(w, r, "/setup", http.StatusSeeOther)
		return
	}
	s.renderAuthPage(w, "login", authPageViewData{Title: "Sign In"})
}

func (s *Server) handleLoginPost(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.EnableAuth {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	hasAdmin, err := s.store.HasAdmin(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !hasAdmin {
		http.Redirect(w, r, "/setup", http.StatusSeeOther)
		return
	}

	username := strings.TrimSpace(r.FormValue("username"))
	password := r.FormValue("password")
	limitKey := clientRateKey(r, username)
	if !s.limiter.Allow(limitKey) {
		w.WriteHeader(http.StatusTooManyRequests)
		s.renderAuthPage(w, "login", authPageViewData{Title: "Sign In", Username: username, Error: "Too many failed login attempts. Try again in a few minutes."})
		return
	}

	u, err := s.store.GetUserByUsername(r.Context(), username)
	if err != nil || !auth.CheckPassword(u.PasswordHash, password) {
		s.limiter.RecordFailure(limitKey)
		w.WriteHeader(http.StatusUnauthorized)
		s.renderAuthPage(w, "login", authPageViewData{Title: "Sign In", Username: username, Error: "Invalid username or password"})
		return
	}

	s.limiter.Reset(limitKey)
	tok, err := auth.NewSessionToken()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	exp := auth.SessionExpiry()
	if err := s.store.CreateSession(r.Context(), tok, u.Username, exp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	setSessionCookie(w, tok, exp)
	_ = s.store.RecordAudit(r.Context(), u.Username, "LOGIN", "dream", "dream", "Signed in to Dream UI")
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie(sessionCookieName); err == nil && cookie.Value != "" {
		_ = s.store.DeleteSession(r.Context(), cookie.Value)
	}
	if u := userFromRequest(r); u != nil {
		_ = s.store.RecordAudit(r.Context(), u.Username, "LOGOUT", "dream", "dream", "Signed out of Dream UI")
	}
	clearSessionCookie(w)
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func (s *Server) handleAuditPage(w http.ResponseWriter, r *http.Request) {
	actionFilter := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("action")))
	stats, err := s.store.GetRunStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	usersRaw, err := s.store.ListUsers(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	logsRaw, err := s.store.ListAuditLogs(r.Context(), 200, actionFilter)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	users := make([]userRowView, 0, len(usersRaw))
	for _, u := range usersRaw {
		users = append(users, userRowView{
			Username:  u.Username,
			IsAdmin:   u.IsAdmin,
			CreatedAt: formatTimestamp(u.CreatedAt),
		})
	}

	logs := make([]auditRowView, 0, len(logsRaw))
	for _, l := range logsRaw {
		logs = append(logs, auditRowView{
			CreatedAt:  formatTimestamp(l.CreatedAt),
			Username:   l.Username,
			Action:     l.Action,
			ActionSlug: auditActionSlug(l.Action),
			Target:     l.Target.String,
			Source:     l.Source,
			Details:    l.Details.String,
			SearchKey:  strings.ToLower(l.Username + " " + l.Action + " " + l.Target.String + " " + l.Source + " " + l.Details.String),
		})
	}

	base := s.newBaseData(r, "Users & Audit", "audit", stats.Running)
	s.renderPage(w, "audit", auditViewData{
		basePageData: base,
		Users:        users,
		AuditLogs:    logs,
		ActionFilter: actionFilter,
		FlashMsg:     r.URL.Query().Get("msg"),
		FlashErr:     r.URL.Query().Get("err"),
	})
}

func (s *Server) handleCreateUser(w http.ResponseWriter, r *http.Request) {
	cur := userFromRequest(r)
	if s.cfg.EnableAuth && (cur == nil || !cur.IsAdmin) {
		http.Error(w, "admin privileges required", http.StatusForbidden)
		return
	}

	username, err := auth.ValidateUsername(r.FormValue("username"))
	if err != nil {
		http.Redirect(w, r, "/audit?err="+url.QueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	hash, err := auth.HashPassword(r.FormValue("password"))
	if err != nil {
		http.Redirect(w, r, "/audit?err="+url.QueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	isAdmin := r.FormValue("is_admin") == "1"

	if _, err := s.store.CreateUser(r.Context(), username, hash, isAdmin); err != nil {
		http.Redirect(w, r, "/audit?err="+url.QueryEscape(err.Error()), http.StatusSeeOther)
		return
	}

	roleLabel := "member"
	if isAdmin {
		roleLabel = "admin"
	}
	_ = s.store.RecordAudit(r.Context(), s.actorName(r), "USER_CREATED", username, "dream", "role="+roleLabel)
	http.Redirect(w, r, "/audit?msg="+url.QueryEscape(fmt.Sprintf("Created user %q (%s)", username, roleLabel)), http.StatusSeeOther)
}

func (s *Server) handleDeleteUser(w http.ResponseWriter, r *http.Request) {
	cur := userFromRequest(r)
	if s.cfg.EnableAuth && (cur == nil || !cur.IsAdmin) {
		http.Error(w, "admin privileges required", http.StatusForbidden)
		return
	}

	target := strings.TrimSpace(r.PathValue("username"))
	if cur != nil && target == cur.Username {
		http.Redirect(w, r, "/audit?err="+url.QueryEscape("Cannot delete your own active account"), http.StatusSeeOther)
		return
	}

	if err := s.store.DeleteUser(r.Context(), target); err != nil {
		msg := err.Error()
		if errors.Is(err, db.ErrLastAdmin) {
			msg = "Cannot delete the last admin account"
		}
		http.Redirect(w, r, "/audit?err="+url.QueryEscape(msg), http.StatusSeeOther)
		return
	}

	_ = s.store.RecordAudit(r.Context(), s.actorName(r), "USER_DELETED", target, "dream", "")
	http.Redirect(w, r, "/audit?msg="+url.QueryEscape(fmt.Sprintf("Deleted user %q", target)), http.StatusSeeOther)
}

func (s *Server) handleChangePassword(w http.ResponseWriter, r *http.Request) {
	cur := userFromRequest(r)
	if cur == nil {
		http.Error(w, "authentication required", http.StatusUnauthorized)
		return
	}

	if !auth.CheckPassword(cur.PasswordHash, r.FormValue("current_password")) {
		http.Redirect(w, r, "/audit?err="+url.QueryEscape("Current password is incorrect"), http.StatusSeeOther)
		return
	}
	newHash, err := auth.HashPassword(r.FormValue("new_password"))
	if err != nil {
		http.Redirect(w, r, "/audit?err="+url.QueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	if err := s.store.UpdateUserPassword(r.Context(), cur.Username, newHash); err != nil {
		http.Redirect(w, r, "/audit?err="+url.QueryEscape(err.Error()), http.StatusSeeOther)
		return
	}

	_ = s.store.RecordAudit(r.Context(), cur.Username, "PASSWORD_CHANGED", cur.Username, "dream", "")
	http.Redirect(w, r, "/audit?msg="+url.QueryEscape("Password updated"), http.StatusSeeOther)
}

func clientRateKey(r *http.Request, username string) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return host + ":" + strings.ToLower(strings.TrimSpace(username))
}

func auditActionSlug(action string) string {
	switch strings.ToUpper(strings.TrimSpace(action)) {
	case "FLOW_RUN", "SETUP_ADMIN", "USER_CREATED":
		return "success"
	case "FLOW_KILL", "USER_DELETED":
		return "killed"
	case "LOGIN", "PASSWORD_CHANGED":
		return "running"
	default:
		return "neutral"
	}
}
