use std::path::{Path, PathBuf};

use crate::args::{Par2Args, Par2Command};

pub(crate) fn run(command: Par2Command) -> Result<i32, Box<dyn std::error::Error>> {
    match command {
        Par2Command::Verify(args) => run_verify(args),
        Par2Command::Repair { args } => run_repair(args),
    }
}

struct ResolvedPar2Input {
    par2_paths: Vec<PathBuf>,
    primary_dir: PathBuf,
    search_dirs: Vec<PathBuf>,
}

fn run_verify(args: Par2Args) -> Result<i32, Box<dyn std::error::Error>> {
    let started = std::time::Instant::now();
    let resolved = resolve_input(&args)?;
    let par2_set = load_par2_set(&resolved.par2_paths)?;

    print_context("verify", &resolved, &par2_set);

    let (verification, placement_plan) = verify_set(&resolved, &par2_set)?;
    print_verification_report(&verification, placement_plan.as_ref(), &par2_set);
    println!("verify completed in {:.2?}", started.elapsed());

    Ok(if verification.total_missing_blocks == 0 {
        0
    } else {
        1
    })
}

fn run_repair(args: Par2Args) -> Result<i32, Box<dyn std::error::Error>> {
    let started = std::time::Instant::now();
    let resolved = resolve_input(&args)?;
    std::fs::create_dir_all(&resolved.primary_dir)?;
    let par2_set = load_par2_set(&resolved.par2_paths)?;

    print_context("repair", &resolved, &par2_set);

    let (verification, placement_plan) = verify_set(&resolved, &par2_set)?;
    println!("pre-repair verification:");
    print_verification_report(&verification, placement_plan.as_ref(), &par2_set);

    match verification.repairable {
        par2_rs::Repairability::NotNeeded => {
            println!("no repair needed");
            println!("repair completed in {:.2?}", started.elapsed());
            return Ok(0);
        }
        par2_rs::Repairability::Insufficient {
            blocks_needed,
            blocks_available,
            deficit,
        } => {
            println!(
                "repair not possible: need {blocks_needed} blocks, have {blocks_available} (deficit {deficit})"
            );
            println!("repair completed in {:.2?}", started.elapsed());
            return Ok(1);
        }
        par2_rs::Repairability::ResourceLimited { reason } => {
            println!("repair not possible: verification resource limit exceeded: {reason}");
            println!("repair completed in {:.2?}", started.elapsed());
            return Ok(1);
        }
        par2_rs::Repairability::Repairable { .. } => {}
    }

    if let Some(plan) = &placement_plan
        && (!plan.swaps.is_empty() || !plan.renames.is_empty())
    {
        let moved = par2_rs::apply_placement_plan(&resolved.primary_dir, plan)?;
        println!("normalized file placement before repair: moved {moved} file(s)");
    }

    let repair_plan = par2_rs::plan_repair(&par2_set, &verification)?;
    println!(
        "repairing {} slice(s) using {} recovery block(s)",
        repair_plan.missing_slices.len(),
        repair_plan.recovery_exponents.len()
    );

    let options = par2_rs::RepairOptions::default();

    let repair_started = std::time::Instant::now();
    let mut repair_access: Box<dyn par2_rs::FileAccess> =
        build_repair_access(&resolved, &par2_set, placement_plan.as_ref());
    par2_rs::execute_repair_with_options(&repair_plan, &par2_set, &mut *repair_access, &options)?;
    println!("repair pass completed in {:.2?}", repair_started.elapsed());

    let post_repair_started = std::time::Instant::now();
    let final_verification = verify_after_repair(&resolved, &par2_set)?;
    println!("post-repair verification:");
    print_verification_report(&final_verification, None, &par2_set);
    println!(
        "post-repair verify completed in {:.2?}",
        post_repair_started.elapsed()
    );
    println!("repair completed in {:.2?}", started.elapsed());

    Ok(if final_verification.total_missing_blocks == 0 {
        0
    } else {
        1
    })
}

fn resolve_input(args: &Par2Args) -> Result<ResolvedPar2Input, Box<dyn std::error::Error>> {
    let input = if args.input.exists() {
        args.input.clone()
    } else {
        return Err(format!("input path does not exist: {}", args.input.display()).into());
    };

    let par2_paths = if input.is_dir() {
        collect_par2_paths_from_dir(&input)?
    } else {
        discover_matching_par2_paths(&input)?
    };

    let primary_dir = args.working_dir.clone().unwrap_or_else(|| {
        if input.is_dir() {
            input.clone()
        } else {
            input
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf()
        }
    });
    validate_directory_path(&primary_dir, "working directory")?;
    for dir in &args.search_dirs {
        validate_directory_path(dir, "search directory")?;
    }

    Ok(ResolvedPar2Input {
        par2_paths,
        primary_dir,
        search_dirs: args.search_dirs.clone(),
    })
}

fn validate_directory_path(path: &Path, label: &str) -> Result<(), Box<dyn std::error::Error>> {
    if path.exists() && !path.is_dir() {
        return Err(format!("{label} is not a directory: {}", path.display()).into());
    }
    Ok(())
}

fn collect_par2_paths_from_dir(dir: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let mut par2_paths = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if name.to_ascii_lowercase().ends_with(".par2") {
            par2_paths.push(entry.path());
        }
    }
    par2_paths.sort();
    if par2_paths.is_empty() {
        return Err(format!("no .par2 files found in {}", dir.display()).into());
    }
    Ok(par2_paths)
}

fn discover_matching_par2_paths(input: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let seed_set = par2_rs::Par2FileSet::from_paths(&[input])?;
    let parent = input.parent().unwrap_or_else(|| Path::new("."));
    let mut par2_paths = par2_rs::identify_par2_files(parent, &seed_set.recovery_set_id)?;
    if par2_paths.is_empty() {
        par2_paths.push(input.to_path_buf());
    }
    par2_paths.sort();
    par2_paths.dedup();
    Ok(par2_paths)
}

fn load_par2_set(
    par2_paths: &[PathBuf],
) -> Result<par2_rs::Par2FileSet, Box<dyn std::error::Error>> {
    Ok(par2_rs::Par2FileSet::from_paths(par2_paths)?)
}

fn verify_set(
    resolved: &ResolvedPar2Input,
    par2_set: &par2_rs::Par2FileSet,
) -> Result<(par2_rs::VerificationResult, Option<par2_rs::PlacementPlan>), Box<dyn std::error::Error>>
{
    if resolved.search_dirs.is_empty() {
        let placement_plan = par2_rs::scan_placement(&resolved.primary_dir, par2_set)?;
        if !placement_plan.conflicts.is_empty() {
            let conflicts = format_conflict_filenames(&placement_plan, par2_set);
            return Err(format!("placement scan found ambiguous matches: {conflicts}").into());
        }
        let access = par2_rs::PlacementFileAccess::from_plan(
            resolved.primary_dir.clone(),
            par2_set,
            &placement_plan,
        );
        let verification = par2_rs::verify_all(par2_set, &access);
        Ok((verification, Some(placement_plan)))
    } else {
        let access = par2_rs::MultiDirectoryFileAccess::new(
            resolved.primary_dir.clone(),
            resolved.search_dirs.clone(),
            par2_set,
        );
        let verification = par2_rs::verify_all(par2_set, &access);
        Ok((verification, None))
    }
}

fn verify_after_repair(
    resolved: &ResolvedPar2Input,
    par2_set: &par2_rs::Par2FileSet,
) -> Result<par2_rs::VerificationResult, Box<dyn std::error::Error>> {
    if resolved.search_dirs.is_empty() {
        let access = par2_rs::DiskFileAccess::new(resolved.primary_dir.clone(), par2_set);
        Ok(par2_rs::verify_all(par2_set, &access))
    } else {
        let access = par2_rs::MultiDirectoryFileAccess::new(
            resolved.primary_dir.clone(),
            resolved.search_dirs.clone(),
            par2_set,
        );
        Ok(par2_rs::verify_all(par2_set, &access))
    }
}

fn build_repair_access(
    resolved: &ResolvedPar2Input,
    par2_set: &par2_rs::Par2FileSet,
    placement_plan: Option<&par2_rs::PlacementPlan>,
) -> Box<dyn par2_rs::FileAccess> {
    if resolved.search_dirs.is_empty() {
        if let Some(plan) = placement_plan
            && plan.swaps.is_empty()
            && plan.renames.is_empty()
        {
            return Box::new(par2_rs::PlacementFileAccess::from_plan(
                resolved.primary_dir.clone(),
                par2_set,
                plan,
            ));
        }
        Box::new(par2_rs::DiskFileAccess::new(
            resolved.primary_dir.clone(),
            par2_set,
        ))
    } else {
        Box::new(par2_rs::MultiDirectoryFileAccess::new(
            resolved.primary_dir.clone(),
            resolved.search_dirs.clone(),
            par2_set,
        ))
    }
}

fn print_context(action: &str, resolved: &ResolvedPar2Input, par2_set: &par2_rs::Par2FileSet) {
    println!("weaver par2 {action}");
    println!("par2 files: {}", resolved.par2_paths.len());
    for path in &resolved.par2_paths {
        println!("  {}", path.display());
    }
    println!("working dir: {}", resolved.primary_dir.display());
    if !resolved.search_dirs.is_empty() {
        println!("search dirs:");
        for dir in &resolved.search_dirs {
            println!("  {}", dir.display());
        }
    }
    println!(
        "par2 set: files={}, slice_size={}, recovery_blocks={}",
        par2_set.files.len(),
        par2_set.slice_size,
        par2_set.recovery_block_count()
    );
}

fn print_verification_report(
    verification: &par2_rs::VerificationResult,
    placement_plan: Option<&par2_rs::PlacementPlan>,
    par2_set: &par2_rs::Par2FileSet,
) {
    if let Some(plan) = placement_plan {
        println!(
            "placement: exact={}, renames={}, swaps={}, unresolved={}, conflicts={}",
            plan.exact.len(),
            plan.renames.len(),
            plan.swaps.len(),
            plan.unresolved.len(),
            plan.conflicts.len()
        );
        for entry in &plan.renames {
            println!("  rename: {} -> {}", entry.current_name, entry.correct_name);
        }
        for (left, right) in &plan.swaps {
            println!("  swap: {} <-> {}", left.current_name, right.current_name);
        }
        if !plan.unresolved.is_empty() {
            let unresolved = plan
                .unresolved
                .iter()
                .filter_map(|file_id| par2_set.file_description(file_id))
                .map(|desc| desc.filename.clone())
                .collect::<Vec<_>>();
            if !unresolved.is_empty() {
                println!("  unresolved: {}", unresolved.join(", "));
            }
        }
    }

    let mut complete = 0usize;
    let mut damaged = 0usize;
    let mut missing = 0usize;
    for file in &verification.files {
        match &file.status {
            par2_rs::FileStatus::Complete => complete += 1,
            par2_rs::FileStatus::Damaged(bad_slices) => {
                damaged += 1;
                println!("  damaged: {} ({} bad slice(s))", file.filename, bad_slices);
            }
            par2_rs::FileStatus::Missing => {
                missing += 1;
                println!(
                    "  missing: {} ({} slice(s))",
                    file.filename, file.missing_slice_count
                );
            }
            par2_rs::FileStatus::Renamed(path) => {
                println!("  renamed: {} -> {}", file.filename, path.display());
            }
        }
    }

    println!(
        "summary: {} complete, {} damaged, {} missing",
        complete, damaged, missing
    );
    println!(
        "missing blocks: {}, recovery blocks available: {}",
        verification.total_missing_blocks, verification.recovery_blocks_available
    );
    match &verification.repairable {
        par2_rs::Repairability::NotNeeded => println!("repairability: not needed"),
        par2_rs::Repairability::Repairable {
            blocks_needed,
            blocks_available,
        } => println!(
            "repairability: repairable (need {}, have {})",
            blocks_needed, blocks_available
        ),
        par2_rs::Repairability::Insufficient {
            blocks_needed,
            blocks_available,
            deficit,
        } => println!(
            "repairability: insufficient (need {}, have {}, deficit {})",
            blocks_needed, blocks_available, deficit
        ),
        par2_rs::Repairability::ResourceLimited { reason } => {
            println!("repairability: resource-limited ({reason})")
        }
    }
}

fn format_conflict_filenames(
    placement_plan: &par2_rs::PlacementPlan,
    par2_set: &par2_rs::Par2FileSet,
) -> String {
    let names: Vec<String> = placement_plan
        .conflicts
        .iter()
        .filter_map(|file_id| par2_set.file_description(file_id))
        .map(|desc| desc.filename.clone())
        .collect();
    if names.is_empty() {
        format!("{} file id(s)", placement_plan.conflicts.len())
    } else {
        names.join(", ")
    }
}
