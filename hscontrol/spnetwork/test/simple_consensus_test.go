package test

import (
	"testing"
	"time"
)

func TestSimpleConsensus(t *testing.T) {
	// Launching with graph visualization
	player := NewScenarioPlayer(9000, true)
	// Cleaning up resources after the test
	defer player.Cleanup()

	steps := []ScenarioStep{
		{At: 0, Type: StepLaunchServer, NodeID: "1", Message: "Launching first (root) server"},
		{At: 1 * time.Second, Type: StepLaunchServer, NodeID: "2", Bootstrap: []string{"1"}, Message: "Launching second server with connection to the first one"},
		{At: 2 * time.Second, Type: StepLaunchServer, NodeID: "3", Bootstrap: []string{"1", "2"}, Message: "Launching third server with connection to the first and second"},
		{At: 3 * time.Second, Type: StepLaunchServer, NodeID: "4", Bootstrap: []string{"1", "2"}, Message: "Launching fourth server with connection to the first and second"},
		//{At: 12 * time.Second, Type: StepLaunchServer, NodeID: "5", Bootstrap: []string{"1", "2"}},
		//{At: 13 * time.Second, Type: StepLaunchServer, NodeID: "6", Bootstrap: []string{"1", "2"}},
		//{At: 14 * time.Second, Type: StepLaunchServer, NodeID: "7", Bootstrap: []string{"1", "2"}},
		//{At: 15 * time.Second, Type: StepLaunchServer, NodeID: "8", Bootstrap: []string{"1", "2"}},
		//
		//{At: 120 * time.Second, Type: StepAddBlacklist, NodeID: "1"},
		{At: 30 * time.Second, Type: StepStopServer, NodeID: "1", Message: "Stopping the leader to test re-election"},
	}

	player.Play(steps)
}
