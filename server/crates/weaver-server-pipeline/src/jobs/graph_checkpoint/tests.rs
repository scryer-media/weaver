// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// The query plan of deleting three keys of one table.
fn delete_plan<R: Row>(db: &Database, keys: &[R::Key]) -> Vec<String> {
    let sql = format!("EXPLAIN QUERY PLAN {}", delete_keys_sql::<R>(keys.len()));
    let mut args = Vec::new();
    for key in keys {
        args.push(SqlArg::I64(7));
        args.extend(R::key_args(key));
    }
    let datastore = db.datastore();
    db.run_sql_blocking_read(async move {
        SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args)
            .await?
            .into_iter()
            .map(|row| row.text("detail"))
            .collect()
    })
    .unwrap()
}

// Every term of a key delete is a seek on the whole primary key, never a
// walk of the job's rows: a job holds tens of thousands of facts parts, and
// each walk read all of them.
fn assert_seeks<R: Row>(db: &Database, keys: &[R::Key]) {
    let plan = delete_plan::<R>(db, keys);
    let searches: Vec<&String> = plan.iter().filter(|line| line.contains(R::TABLE)).collect();
    assert!(!searches.is_empty(), "{}: no plan: {plan:?}", R::TABLE);
    for line in searches {
        assert!(
            line.starts_with("SEARCH") && line.contains("job_id=? AND"),
            "{}: a delete walks the job's rows: {plan:?}",
            R::TABLE
        );
    }
}

#[test]
fn a_key_delete_seeks_each_key_in_every_table() {
    let db = Database::open_in_memory().unwrap();
    assert_seeks::<NodeRow>(&db, &[1, 2, 3]);
    assert_seeks::<MemberRow>(&db, &[(1, 2), (3, 4), (5, 6)]);
    assert_seeks::<CoverageRow>(&db, &[(1, 2, 3), (4, 5, 6), (7, 8, 9)]);
    assert_seeks::<EnvelopeRow>(&db, &[(1, 2, 3), (4, 5, 6), (7, 8, 9)]);
    assert_seeks::<CryptRow>(&db, &[(1, 2, 3), (4, 5, 6), (7, 8, 9)]);
    assert_seeks::<HoldRow>(&db, &[(1, 2, 3), (4, 5, 6), (7, 8, 9)]);
    assert_seeks::<PartRow>(&db, &[(1, 2), (3, 4), (5, 6)]);
    assert_seeks::<HeldPartRow>(&db, &[(1, 2), (3, 4), (5, 6)]);
}

// The statement the key delete replaced: the job's id once, outside the
// terms.
fn job_outside_terms_sql(keys: usize) -> String {
    format!(
        "DELETE FROM graph_facts_parts WHERE job_id = {{}} AND ({})",
        vec!["(node_id = {} AND part = {})"; keys].join(" OR ")
    )
}

fn seeded() -> Database {
    let db = Database::open_in_memory().unwrap();
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::run_in_transaction(&datastore, "seed_graph_parts", |tx| {
            Box::pin(async move {
                for job in 1..=3_i64 {
                    for node in 0..40_i64 {
                        let rows: Vec<String> = (0..50_i64)
                            .map(|part| format!("({job}, {node}, {part}, x'{:02x}')", part))
                            .collect();
                        tx.execute(
                            &format!(
                                "INSERT INTO graph_facts_parts (job_id, node_id, part, bytes) VALUES {}",
                                rows.join(", ")
                            ),
                            &[],
                        )
                        .await?;
                    }
                }
                Ok(())
            })
        })
        .await
    })
    .unwrap();
    db
}

fn parts(db: &Database) -> Vec<(i64, i64, i64, Vec<u8>)> {
    let datastore = db.datastore();
    db.run_sql_blocking_read(async move {
        SqlRuntime::fetch_all(
            datastore.read_exec(),
            "SELECT job_id, node_id, part, bytes FROM graph_facts_parts ORDER BY job_id, node_id, part",
            &[],
        )
        .await?
        .into_iter()
        .map(|row| {
            Ok((
                row.i64("job_id")?,
                row.i64("node_id")?,
                row.i64("part")?,
                row.bytes("bytes")?,
            ))
        })
        .collect()
    })
    .unwrap()
}

fn run_delete(db: &Database, sql: String, args: Vec<SqlArg>) -> u64 {
    let datastore = db.datastore();
    db.run_sql_blocking(
        async move { SqlRuntime::execute(datastore.write_exec(), &sql, &args).await },
    )
    .unwrap()
}

// The key delete removes exactly the rows the statement it replaced did,
// on seeded rows of three jobs, with keys that match, keys of other jobs'
// rows and keys no row has.
#[test]
fn a_key_delete_removes_what_the_job_wide_delete_did() {
    let keys: Vec<(i64, i64)> = (0..64_i64)
        .map(|index| ((index * 7) % 45, (index * 13) % 55))
        .collect();
    let old = seeded();
    let new = seeded();
    assert_eq!(parts(&old), parts(&new));

    let mut old_args = vec![SqlArg::I64(2)];
    old_args.extend(keys.iter().flat_map(PartRow::key_args));
    let old_deleted = run_delete(&old, job_outside_terms_sql(keys.len()), old_args);

    let mut new_args = Vec::new();
    for key in &keys {
        new_args.push(SqlArg::I64(2));
        new_args.extend(PartRow::key_args(key));
    }
    let new_deleted = run_delete(&new, delete_keys_sql::<PartRow>(keys.len()), new_args);

    assert!(old_deleted > 0, "the keys matched nothing");
    assert_eq!(old_deleted, new_deleted);
    assert_eq!(parts(&old), parts(&new));
}
