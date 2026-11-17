//go:build cgo

package automation

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestLocationActivityPersistsUntilCleared(t *testing.T) {
	var activity []bool
	var h *Host
	h = New(Options{
		Directory: t.TempDir(),
		OnSimulatedLocationChanged: func(active bool) {
			// The observer can query the committed state without taking a lock
			// already held by the publisher.
			location, _ := h.currentLocation()
			if (location != nil) != active {
				t.Errorf("activity=%v disagrees with committed location=%v", active, location)
			}
			activity = append(activity, active)
		},
	})
	defer h.Close()
	for _, latitude := range []float64{41.42, 42.42} {
		if err := h.storeLocation(&simulatedLocation{Owner: "simulate-location", Latitude: latitude, Longitude: -107.05}); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(activity, []bool{true}) {
		t.Fatalf("location must remain active between actions: %v", activity)
	}
	if _, err := os.Stat(filepath.Join(h.options.Directory, locationFile)); err != nil {
		t.Fatal(err)
	}
	if err := h.storeLocation(nil); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(activity, []bool{true, false}) {
		t.Fatalf("activity after stopping = %v", activity)
	}
}

func TestFailedLocationSaveDoesNotPublishActivity(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(directory, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	h := New(Options{Directory: directory, OnSimulatedLocationChanged: func(bool) {
		t.Error("failed location save published an activity change")
	}})
	defer h.Close()
	if err := h.storeLocation(&simulatedLocation{Owner: "simulate-location"}); err == nil {
		t.Fatal("expected the location save to fail")
	}
	if location, _ := h.currentLocation(); location != nil {
		t.Fatalf("failed save changed location state: %v", location)
	}
}
