package main

import "testing"

func TestChooseExplainClause_DefaultsAndConfig(t *testing.T) {
	// Defaults when no config provided
	if got := chooseExplainClause(nil, "select 1"); got == "" || got == "EXPLAIN (FORMAT TEXT)" {
		t.Errorf("expected ANALYZE default for SELECT, got %q", got)
	}
	if got := chooseExplainClause(nil, "update x set a=1"); got != "EXPLAIN (FORMAT TEXT)" {
		t.Errorf("expected non-ANALYZE default for non-SELECT, got %q", got)
	}

	cfg := &Config{}
	cfg.Comparison.ExplainSelect = "EXPLAIN (ANALYZE, COSTS OFF)"
	cfg.Comparison.ExplainOther = "EXPLAIN (VERBOSE)"

	if got := chooseExplainClause(cfg, "WITH cte AS (SELECT 1) SELECT * FROM cte"); got != cfg.Comparison.ExplainSelect {
		t.Errorf("custom select clause not applied, got %q", got)
	}
	if got := chooseExplainClause(cfg, "insert into t values (1)"); got != cfg.Comparison.ExplainOther {
		t.Errorf("custom other clause not applied, got %q", got)
	}
}

func TestIsDDLOrUtility(t *testing.T) {
	ddl := []string{"CREATE TABLE t(id int)", "Alter table t add column x int", "drop table t", "BEGIN", "COMMIT", "EXPLAIN select 1", "VACUUM", "SHOW work_mem"}
	for _, q := range ddl {
		if !isDDLOrUtility(q) {
			t.Errorf("expected %q to be treated as DDL/utility", q)
		}
	}
	non := []string{"select 1", " with a as (select 1) select * from a ", "update t set x=1", "delete from t", "insert into t values(1)"}
	for _, q := range non {
		if isDDLOrUtility(q) {
			t.Errorf("did not expect %q to be treated as DDL/utility", q)
		}
	}
}
