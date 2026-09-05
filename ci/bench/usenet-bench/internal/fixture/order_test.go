package fixture

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
)

func volumeSet(count int) []string {
	files := make([]string, 0, count+2)
	for index := 1; index <= count; index++ {
		files = append(files, fmt.Sprintf("archive/fixture.part%02d.rar", index))
	}
	return append(files, "archive/fixture.par2", "archive/fixture.vol000+01.par2")
}

func TestSequentialOrderIsSortedAndUnseeded(t *testing.T) {
	files := volumeSet(6)
	ordered, seed, err := OrderedNZBFiles(SequentialNZBOrder, "any-fixture", files)
	if err != nil {
		t.Fatal(err)
	}
	if seed != 0 {
		t.Fatalf("sequential order seed = %d, want 0", seed)
	}
	want := append([]string(nil), files...)
	sort.Strings(want)
	if !reflect.DeepEqual(ordered, want) {
		t.Fatalf("sequential order = %v, want %v", ordered, want)
	}
}

func TestScatteredOrderIsStableForTheSameFixture(t *testing.T) {
	files := volumeSet(8)
	first, firstSeed, err := OrderedNZBFiles(ScatteredNZBOrder, "bench-fixture-a", files)
	if err != nil {
		t.Fatal(err)
	}
	// Feed the second call a differently ordered input to prove the
	// permutation depends on the fixture id, not on argument order.
	shuffledInput := append([]string(nil), files...)
	for i, j := 0, len(shuffledInput)-1; i < j; i, j = i+1, j-1 {
		shuffledInput[i], shuffledInput[j] = shuffledInput[j], shuffledInput[i]
	}
	second, secondSeed, err := OrderedNZBFiles(ScatteredNZBOrder, "bench-fixture-a", shuffledInput)
	if err != nil {
		t.Fatal(err)
	}
	if firstSeed != secondSeed {
		t.Fatalf("scattered seed is unstable: %d then %d", firstSeed, secondSeed)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("scattered order is unstable:\n%v\n%v", first, second)
	}
	if firstSeed != NZBOrderSeed("bench-fixture-a") {
		t.Fatalf("scattered seed %d is not derived from the fixture id", firstSeed)
	}
}

func TestScatteredOrderDiffersBetweenFixturesAndFromSequential(t *testing.T) {
	files := volumeSet(8)
	sequential, _, err := OrderedNZBFiles(SequentialNZBOrder, "bench-fixture-a", files)
	if err != nil {
		t.Fatal(err)
	}
	first, _, err := OrderedNZBFiles(ScatteredNZBOrder, "bench-fixture-a", files)
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := OrderedNZBFiles(ScatteredNZBOrder, "bench-fixture-b", files)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(first, sequential) {
		t.Fatalf("scattered order matches sequential order: %v", first)
	}
	if reflect.DeepEqual(first, second) {
		t.Fatalf("two fixtures share one scattered order: %v", first)
	}
}

func TestScatteredOrderInterleavesRepairMaterial(t *testing.T) {
	for _, id := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		ordered, _, err := OrderedNZBFiles(ScatteredNZBOrder, "bench-fixture-"+id, volumeSet(8))
		if err != nil {
			t.Fatal(err)
		}
		firstRepair, lastVolume := -1, -1
		for index, name := range ordered {
			switch {
			case len(name) > 5 && name[len(name)-5:] == ".par2":
				if firstRepair < 0 {
					firstRepair = index
				}
			default:
				lastVolume = index
			}
		}
		if firstRepair < 0 || lastVolume < 0 {
			t.Fatalf("fixture %s produced no repair or volume entries: %v", id, ordered)
		}
		if firstRepair > lastVolume {
			t.Fatalf("fixture %s left repair material trailing every volume: %v", id, ordered)
		}
	}
}

func TestScatteredOrderIsAPermutation(t *testing.T) {
	files := volumeSet(12)
	ordered, _, err := OrderedNZBFiles(ScatteredNZBOrder, "bench-fixture-permutation", files)
	if err != nil {
		t.Fatal(err)
	}
	if len(ordered) != len(files) {
		t.Fatalf("scattered order has %d entries, want %d", len(ordered), len(files))
	}
	got := append([]string(nil), ordered...)
	want := append([]string(nil), files...)
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("scattered order is not a permutation of its input")
	}
}

func TestOrderedNZBFilesRejectsUnknownOrder(t *testing.T) {
	if _, _, err := OrderedNZBFiles(NZBOrder("random"), "fixture", volumeSet(2)); err == nil {
		t.Fatal("expected an error for an unsupported nzb_order")
	}
}
