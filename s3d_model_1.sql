-- Step 1: Extract flattened dependency data
WITH project_dependency AS (
  SELECT
    sbom.documentNamespace AS project_id,
    pkg.name AS dependency_name,
    pkg.versionInfo AS dependency_version
  FROM `s3d_dura_data.sboms` t
  CROSS JOIN UNNEST(sbom.packages) AS pkg
  WHERE pkg.name IS NOT NULL
),

-- Step 2: Count total distinct projects
total_projects AS (
  SELECT COUNT(DISTINCT project_id) AS total_project_count
  FROM project_dependency
),

-- Step 3: Name-level distribution
name_distribution AS (
  SELECT
    'name' AS metric_level,
    dependency_name AS dependency_id,
    COUNT(DISTINCT project_id) AS project_count
  FROM project_dependency
  GROUP BY dependency_name
),

-- Step 4: Version-level distribution
version_distribution AS (
  SELECT
    'version' AS metric_level,
    CONCAT(dependency_name, '@', dependency_version) AS dependency_id,
    COUNT(DISTINCT project_id) AS project_count
  FROM project_dependency
  WHERE dependency_version IS NOT NULL
  GROUP BY dependency_name, dependency_version
),

-- Step 5: Combine name and version metrics
combined_distribution AS (
  SELECT * FROM name_distribution
  UNION ALL
  SELECT * FROM version_distribution
),

-- Step 6: Calculate relative distribution
distribution_metric AS (
  SELECT
    cd.metric_level,
    cd.dependency_id,
    cd.project_count,
    tp.total_project_count,
    SAFE_DIVIDE(cd.project_count, tp.total_project_count) AS relative_distribution
  FROM combined_distribution cd
  CROSS JOIN total_projects tp
)

-- Step 7: Add percentile rank within each metric type
SELECT
  metric_level,
  dependency_id,
  project_count,
  total_project_count,
  relative_distribution,
  PERCENT_RANK() OVER (PARTITION BY metric_level ORDER BY relative_distribution) AS percentile_rank,
  CURRENT_DATE() AS run_date
FROM distribution_metric
ORDER BY metric_level, relative_distribution DESC;