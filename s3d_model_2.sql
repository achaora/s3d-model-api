-- Updated: include rows with NULL versionInfo for accurate name counts,
-- and label NULL versions as '(unknown)' for version-level metrics.

WITH project_dependency AS (
  SELECT
    t.sbom.documentNamespace AS project_id,
    pkg.name AS dependency_name,
    pkg.versionInfo AS dependency_version
  FROM `s3d_dura_data.sboms` AS t
  CROSS JOIN UNNEST(t.sbom.packages) AS pkg
  WHERE pkg.name IS NOT NULL
),

total_projects AS (
  SELECT COUNT(DISTINCT project_id) AS total_project_count
  FROM project_dependency
),

-- Name-level distribution (includes all rows regardless of versionInfo)
name_distribution AS (
  SELECT
    dependency_name,
    COUNT(DISTINCT project_id) AS project_count_name
  FROM project_dependency
  GROUP BY dependency_name
),

-- Name-level metrics with relative distribution
name_metrics AS (
  SELECT
    nd.dependency_name,
    nd.project_count_name,
    tp.total_project_count,
    SAFE_DIVIDE(nd.project_count_name, tp.total_project_count) AS relative_distribution_name
  FROM name_distribution nd
  CROSS JOIN total_projects tp
),

-- Percentiles computed across unique dependency names
name_metrics_with_percentile AS (
  SELECT
    dependency_name,
    project_count_name,
    total_project_count,
    relative_distribution_name,
    PERCENT_RANK() OVER (ORDER BY relative_distribution_name) AS percentile_rank_name
  FROM name_metrics
),

-- Version-level distribution: treat NULL versions as '(unknown)'
version_distribution AS (
  SELECT
    dependency_name,
    COALESCE(dependency_version, '(unknown)') AS dependency_version,
    COUNT(DISTINCT project_id) AS project_count_version
  FROM project_dependency
  GROUP BY dependency_name, COALESCE(dependency_version, '(unknown)')
),

-- Version-level metrics with relative distribution
version_metrics AS (
  SELECT
    vd.dependency_name,
    vd.dependency_version,
    vd.project_count_version,
    tp.total_project_count,
    SAFE_DIVIDE(vd.project_count_version, tp.total_project_count) AS relative_distribution_version
  FROM version_distribution vd
  CROSS JOIN total_projects tp
),

-- Percentiles computed across all dependency-version rows
version_metrics_with_percentile AS (
  SELECT
    dependency_name,
    dependency_version,
    project_count_version,
    total_project_count,
    relative_distribution_version,
    PERCENT_RANK() OVER (ORDER BY relative_distribution_version) AS percentile_rank_version
  FROM version_metrics
)

-- Final: join name-level metrics (one row per name) to version-level rows
SELECT
  CURRENT_DATE() AS run_date,
  nm.dependency_name,
  vm.dependency_version,
  -- Name-level metrics (side-by-side)
  nm.project_count_name,
  nm.relative_distribution_name,
  nm.percentile_rank_name,
  -- Version-level metrics (side-by-side)
  vm.project_count_version,
  vm.relative_distribution_version,
  vm.percentile_rank_version
FROM version_metrics_with_percentile AS vm
JOIN name_metrics_with_percentile AS nm
  ON vm.dependency_name = nm.dependency_name
ORDER BY nm.relative_distribution_name DESC, vm.dependency_name, vm.dependency_version;