package ui

import (
	"fmt"
	"strings"
)

// DAGTaskInput describes a task node for SVG DAG layout calculation.
type DAGTaskInput struct {
	ID        string
	Runtime   string
	Image     string
	DependsOn []string
	Status    string
}

// DAGNode holds computed SVG coordinates and metadata for a task box.
type DAGNode struct {
	ID        string
	Runtime   string
	Image     string
	Status    string
	StatusCls string
	X         int
	Y         int
	Width     int
	Height    int
	CenterY   int
	RightX    int
}

// DAGEdge holds an SVG cubic Bezier path between two task nodes.
type DAGEdge struct {
	From string
	To   string
	Path string
}

// DAGGraph holds the full computed SVG layout.
type DAGGraph struct {
	Width  int
	Height int
	Nodes  []DAGNode
	Edges  []DAGEdge
}

// BuildDAGLayout computes a left-to-right layered SVG DAG layout.
func BuildDAGLayout(tasks []DAGTaskInput) DAGGraph {
	if len(tasks) == 0 {
		return DAGGraph{Width: 300, Height: 100}
	}

	const (
		nodeW  = 196
		nodeH  = 64
		colGap = 72
		rowGap = 24
		padX   = 24
		padY   = 24
	)

	byID := make(map[string]DAGTaskInput, len(tasks))
	for _, t := range tasks {
		byID[t.ID] = t
	}

	// Compute longest-path layer index for each task.
	layerCache := make(map[string]int, len(tasks))
	visiting := make(map[string]bool, len(tasks))
	var getLayer func(id string) int
	getLayer = func(id string) int {
		if l, ok := layerCache[id]; ok {
			return l
		}
		if visiting[id] {
			return 0
		}
		visiting[id] = true
		t, ok := byID[id]
		maxParent := -1
		if ok {
			for _, dep := range t.DependsOn {
				if _, exists := byID[dep]; !exists {
					continue
				}
				if pl := getLayer(dep); pl > maxParent {
					maxParent = pl
				}
			}
		}
		delete(visiting, id)
		layer := maxParent + 1
		layerCache[id] = layer
		return layer
	}

	maxLayer := 0
	columns := make(map[int][]DAGTaskInput)
	for _, t := range tasks {
		l := getLayer(t.ID)
		if l > maxLayer {
			maxLayer = l
		}
		columns[l] = append(columns[l], t)
	}

	maxRows := 1
	for _, col := range columns {
		if len(col) > maxRows {
			maxRows = len(col)
		}
	}

	totalHeight := padY*2 + maxRows*nodeH + (maxRows-1)*rowGap
	totalWidth := padX*2 + (maxLayer+1)*nodeW + maxLayer*colGap

	nodeMap := make(map[string]DAGNode, len(tasks))
	nodes := make([]DAGNode, 0, len(tasks))

	for colIdx := 0; colIdx <= maxLayer; colIdx++ {
		colTasks := columns[colIdx]
		colHeight := len(colTasks)*nodeH + max(0, len(colTasks)-1)*rowGap
		startY := padY + (totalHeight-padY*2-colHeight)/2
		x := padX + colIdx*(nodeW+colGap)

		for rowIdx, t := range colTasks {
			y := startY + rowIdx*(nodeH+rowGap)
			node := DAGNode{
				ID:        t.ID,
				Runtime:   t.Runtime,
				Image:     t.Image,
				Status:    t.Status,
				StatusCls: statusSlug(t.Status),
				X:         x,
				Y:         y,
				Width:     nodeW,
				Height:    nodeH,
				CenterY:   y + nodeH/2,
				RightX:    x + nodeW,
			}
			nodeMap[t.ID] = node
			nodes = append(nodes, node)
		}
	}

	var edges []DAGEdge
	for _, t := range tasks {
		toNode, okTo := nodeMap[t.ID]
		if !okTo {
			continue
		}
		for _, dep := range t.DependsOn {
			fromNode, okFrom := nodeMap[dep]
			if !okFrom {
				continue
			}
			x1 := fromNode.RightX
			y1 := fromNode.CenterY
			x2 := toNode.X
			y2 := toNode.CenterY
			dx := (x2 - x1) / 2
			if dx < 28 {
				dx = 28
			}
			path := fmt.Sprintf("M %d %d C %d %d, %d %d, %d %d", x1, y1, x1+dx, y1, x2-dx, y2, x2, y2)
			edges = append(edges, DAGEdge{
				From: dep,
				To:   t.ID,
				Path: path,
			})
		}
	}

	return DAGGraph{
		Width:  totalWidth,
		Height: totalHeight,
		Nodes:  nodes,
		Edges:  edges,
	}
}

func statusSlug(status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "SUCCESS":
		return "success"
	case "FAILED":
		return "failed"
	case "RUNNING":
		return "running"
	case "KILLED":
		return "killed"
	case "SKIPPED":
		return "skipped"
	case "PENDING":
		return "pending"
	default:
		return "neutral"
	}
}
