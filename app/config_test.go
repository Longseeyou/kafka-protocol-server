package main

import "testing"

func TestConfiguredMetadata(t *testing.T) {
	first, err := configuredMetadata("orders:3,events:2")
	if err != nil {
		t.Fatal(err)
	}
	metadata := extractClusterMetadata(&first)
	if len(metadata.Topics) != 2 || len(metadata.Topics["orders"].Partitions) != 3 {
		t.Fatal("incorrect topics")
	}
	second, err := configuredMetadata("orders:3")
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Topics["orders"].TopicID != extractClusterMetadata(&second).Topics["orders"].TopicID {
		t.Fatal("unstable topic ID")
	}
	for _, input := range []string{"", "../bad:1", "a:0", "a:1001", "a:1,a:2", "a:no"} {
		if _, err := configuredMetadata(input); err == nil {
			t.Errorf("accepted %q", input)
		}
	}
}
