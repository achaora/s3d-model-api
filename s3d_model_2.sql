-- Step 1: Flatten the data
WITH project_dependency AS (
  SELECT
    sbom.documentNamespace AS project_id,
    pkg.name AS dependency_name,
    pkg.versionInfo AS dependency_version
  FROM `s3d_dura_data.sboms` t
  CROSS JOIN UNNEST(sbom.packages) AS pkg
  WHERE pkg.name IS NOT NULL
    --AND pkg.versionInfo IS NOT NULL
),

-- Step 2: Total number of projects
total_projects AS (
  SELECT COUNT(DISTINCT project_id) AS total_project_count
  FROM project_dependency
),

-- Step 3: Name-level metrics
name_distribution AS (
  SELECT
    dependency_name,
    COUNT(DISTINCT project_id) AS project_count_name
  FROM project_dependency
  GROUP BY dependency_name
),

-- Step 4: Version-level metrics
version_distribution AS (
  SELECT
    dependency_name,
    dependency_version,
    COUNT(DISTINCT project_id) AS project_count_version
  FROM project_dependency
  GROUP BY dependency_name, dependency_version
),

-- Step 5: Combine both metrics
combined_metrics AS (
  SELECT
    vd.dependency_name,
    vd.dependency_version,
    nd.project_count_name,
    SAFE_DIVIDE(nd.project_count_name, tp.total_project_count) AS relative_distribution_name,
    PERCENT_RANK() OVER (ORDER BY SAFE_DIVIDE(nd.project_count_name, tp.total_project_count)) AS percentile_rank_name,
    vd.project_count_version,
    SAFE_DIVIDE(vd.project_count_version, tp.total_project_count) AS relative_distribution_version,
    PERCENT_RANK() OVER (ORDER BY SAFE_DIVIDE(vd.project_count_version, tp.total_project_count)) AS percentile_rank_version
  FROM version_distribution vd
  JOIN name_distribution nd
    ON vd.dependency_name = nd.dependency_name
  CROSS JOIN total_projects tp
)

-- Step 6: Output with side-by-side metric groups
SELECT
  dependency_name,
  dependency_version,
  -- Name-level metrics
  project_count_name,
  relative_distribution_name,
  percentile_rank_name,
  -- Version-level metrics
  project_count_version,
  relative_distribution_version,
  percentile_rank_version,
  CURRENT_DATE() AS run_date
FROM combined_metrics
ORDER BY relative_distribution_name DESC, dependency_version;
