package test

import (
	"github.com/juanfont/headscale/hscontrol/spnetwork/common/entities"
	"testing"
	"time"
)

func TestGrouping(t *testing.T) {
	// Launching with graph visualization
	player := NewScenarioPlayer(9000, true)
	// Cleaning up resources after the test
	defer player.Cleanup()

	steps := []ScenarioStep{
		{At: 0, Type: StepLaunchServer, NodeID: "1", Message: "Launching first server (will be consensus leader)"},
		{At: 1 * time.Second, Type: StepLaunchServer, NodeID: "2", Bootstrap: []string{"1"}, Message: "Launching second server"},
		{At: 2 * time.Second, Type: StepLaunchServer, NodeID: "3", Bootstrap: []string{"1", "2"}, Message: "Launching third server"},
		{At: 3 * time.Second, Type: StepLaunchServer, NodeID: "4", Bootstrap: []string{"1", "2"}, Message: "Launching fourth server"},

		{At: 4 * time.Second, Type: StepAddGoal, NodeID: "1", GoalID: "kubernetes",
			MinGroupSize: 2, MaxGroupSize: 7, InactiveTimeout: 60,
			Criteria: []entities.DimensionCriterion{
				{
					Type:      entities.LatencyClass,
					Condition: entities.ConditionMin,
					Values:    []float64{},
				},
			},
			Message: "Adding grouping goal 'kubernetes' with minimal latency",
		},

		{At: 12 * time.Second, Type: StepLaunchServer, NodeID: "5", Bootstrap: []string{"1", "2"}, Message: "Network expansion: launching server #5"},
		{At: 13 * time.Second, Type: StepLaunchServer, NodeID: "6", Bootstrap: []string{"1", "2"}, Message: "Network expansion: launching server #6"},
		{At: 14 * time.Second, Type: StepLaunchServer, NodeID: "7", Bootstrap: []string{"1", "2"}, Message: "Network expansion: launching server #7"},
		{At: 15 * time.Second, Type: StepLaunchServer, NodeID: "8", Bootstrap: []string{"1", "2"}, Message: "Network expansion: launching server #8"},
		{At: 16 * time.Second, Type: StepLaunchServer, NodeID: "9", Bootstrap: []string{"1", "2"}, Message: "Network expansion: launching server #9"},
		{At: 17 * time.Second, Type: StepLaunchServer, NodeID: "10", Bootstrap: []string{"1", "2"}, Message: "Network expansion: launching server #10"},

		{At: 18 * time.Second, Type: StepAddGoal, NodeID: "1", GoalID: "storage",
			MinGroupSize: 2, MaxGroupSize: 7, InactiveTimeout: 60,
			Criteria: []entities.DimensionCriterion{
				{
					Type:      entities.LatencyClass,
					Condition: entities.ConditionMin,
					Values:    []float64{},
				},
				{
					Type:      entities.LatencyClass,
					Condition: entities.ConditionBetween,
					Values:    []float64{0, 3},
				},
			},
			Message: "Adding grouping goal 'storage' with two latency criteria",
		},

		{At: 45 * time.Second, Type: StepAddBlacklist, NodeID: "1", Message: "Make server #1 unavailable"},
		{At: 55 * time.Second, Type: StepAddBlacklist, NodeID: "2", Message: "Make server #2 unavailable"},
		{At: 65 * time.Second, Type: StepAddBlacklist, NodeID: "3", Message: "Make server #3 unavailable"},
		{At: 75 * time.Second, Type: StepAddBlacklist, NodeID: "4", Message: "Make server #4 unavailable"},
		{At: 85 * time.Second, Type: StepAddBlacklist, NodeID: "5", Message: "Make server #5 unavailable"},
		{At: 95 * time.Second, Type: StepAddBlacklist, NodeID: "6", Message: "Make server #6 unavailable"},
		{At: 96 * time.Second, Type: StepAddBlacklist, NodeID: "7", Message: "Make server #7 unavailable"},
		{At: 97 * time.Second, Type: StepAddBlacklist, NodeID: "8", Message: "Make server #8 unavailable"},

		//
		//{At: 120 * time.Second, Type: StepAddBlacklist, NodeID: "1"},
		{At: 240 * time.Second, Type: StepStopServer, NodeID: "1", Message: "Stopping leader to check re-election and group stability"},
	}

	player.Play(steps)
}
