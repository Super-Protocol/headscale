package dnetwork

import (
	"fmt"
	"os"
	"strings"
)

func (g *DGraph) ExportToDOT(filename string, measurementName string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create DOT file: %w", err)
	}
	defer file.Close()

	_, err = file.WriteString("digraph DNetwork {\n")
	if err != nil {
		return fmt.Errorf("failed to write to DOT file: %w", err)
	}

	// Write nodes
	for _, node := range g.GetNodes() {
		_, err = file.WriteString(fmt.Sprintf("  \"%s:%d\";\n", node.Host, node.Port))
		if err != nil {
			return fmt.Errorf("failed to write node to DOT file: %w", err)
		}
	}

	edges := g.g.Edges()

	// Write edges
	for edges.Next() {
		edge := edges.Edge()
		dEdge, ok := edge.(*DEdge)
		if !ok {
			continue
		}

		// Собираем все метки в одну строку
		var labelParts []string
		haveNeededMeasurement := false
		for name, value := range dEdge.measurements {
			labelParts = append(labelParts, fmt.Sprintf("%s:%v", name, value.Value))
			if name == measurementName {
				haveNeededMeasurement = true
			}
		}
		if !haveNeededMeasurement {
			continue
		}
		label := ""
		if len(labelParts) > 0 {
			label = fmt.Sprintf(" [label=\"%s\"]", strings.Join(labelParts, ", "))
		}

		_, err = file.WriteString(fmt.Sprintf("  \"%s:%d\" -> \"%s:%d\"%s;\n",
			dEdge.from.Host, dEdge.from.Port,
			dEdge.to.Host, dEdge.to.Port,
			label))
		if err != nil {
			return fmt.Errorf("failed to write edge to DOT file: %w", err)
		}
	}

	_, err = file.WriteString("}\n")
	if err != nil {
		return fmt.Errorf("failed to finalize DOT file: %w", err)
	}

	return nil
}
