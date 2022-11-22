package borg

import "testing"

func TestFindConsulRepo(t *testing.T) {

	cr, err := FindConsulRepo("blargh")

	if err != nil {
		t.Errorf("Got err: %s", err.Error())
		return
	}

	if cr == nil {
		t.Errorf("Got null consul repo.")
		return
	}

	deleteRepoErr := cr.Delete()

	if deleteRepoErr != nil {
		t.Errorf("Failed to delete test consulRepo: %s", deleteRepoErr.Error())
	}

}
