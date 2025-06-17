# Databricks notebook source
# Load required packages
library(survival)
library(dplyr)
library(tidyr)
library(purrr)
library(tibble)

data <- read.csv("near_miss_data_export_v2.csv")

# COMMAND ----------

# Check number of unique EIDs
length(unique(data$ scopus_id))

# COMMAND ----------

# MAGIC %r # Assuming data is your dataframe with all metrics and is_cited_in_policy
# MAGIC
# MAGIC # List of metrics to analyze
# MAGIC metrics <- c("CiteScore", "academic_citations_adjusted", "max_h_index", 
# MAGIC             "collaboration_global_south", "collaboration_withGovt_authors", 
# MAGIC             "media_mentions", "team_past_policy_impact", "num_countries")
# MAGIC
# MAGIC # Function to calculate mean for cited and non-cited papers
# MAGIC calculate_means <- function(metric) {
# MAGIC  # Skip if the metric doesn't exist in the dataframe
# MAGIC  if (!(metric %in% names(data))) {
# MAGIC    return(c(NA, NA))
# MAGIC  }
# MAGIC  
# MAGIC  # Calculate means, handling NA values
# MAGIC  mean_cited <- mean(data[[metric]][data$ cited_in_policy == 1], na.rm = TRUE)
# MAGIC  mean_not_cited <- mean(data[[metric]][data$ cited_in_policy == 0], na.rm = TRUE)
# MAGIC  
# MAGIC  return(c(mean_cited, mean_not_cited))
# MAGIC }
# MAGIC
# MAGIC # Create results dataframe
# MAGIC results <- data.frame(
# MAGIC  metric = metrics,
# MAGIC  mean_cited = NA,
# MAGIC  mean_not_cited = NA,
# MAGIC  difference = NA,
# MAGIC  ratio = NA
# MAGIC )
# MAGIC
# MAGIC # Calculate metrics for each variable
# MAGIC for (i in 1:length(metrics)) {
# MAGIC  means <- calculate_means(metrics[i])
# MAGIC  results$mean_cited[i] <- means[1]
# MAGIC  results$mean_not_cited[i] <- means[2]
# MAGIC  results$difference[i] <- means[1] - means[2]
# MAGIC  results$ratio[i] <- means[1] / means[2]
# MAGIC }
# MAGIC
# MAGIC # Print results
# MAGIC print(results)

# COMMAND ----------

# ─── 3. Define your metrics ────────────────────────────────────────────────────
metrics <- c(
  "CiteScore",
  "academic_citations_adjusted",
  "max_h_index",
  "collaboration_global_south",
  "collaboration_withGovt_authors",
  "media_mentions",
  "team_past_policy_impact",
  "num_countries"
)

# ─── 4. Pivot & within-pair compare ────────────────────────────────────────────
summary_table <- data %>%
  pivot_longer(all_of(metrics), names_to = "metric", values_to = "value") %>%
  group_by(pair_id, metric) %>%
  summarise(
    cited_value    = value[cited_in_policy == 1][1],
    noncited_value = value[cited_in_policy == 0][1],
    .groups = "drop"
  ) %>%
  filter(!is.na(cited_value) & !is.na(noncited_value)) %>%
  mutate(
    result = case_when(
      cited_value  > noncited_value ~ "cited_higher",
      cited_value  < noncited_value ~ "noncited_higher",
      TRUE                          ~ "equal"
    )
  ) %>%
  count(metric, result) %>%
  group_by(metric) %>%
  mutate(pct = n / sum(n) * 100) %>%
  ungroup()

# ─── 5. Print full table ───────────────────────────────────────────────────────
print(summary_table, n = Inf, width = Inf)

# COMMAND ----------


# Function to analyze Global South authorship patterns
analyze_global_south <- function(df) {
 # Remove rows with NA values for global_south_author
 df_valid <- df[!is.na(df$collaboration_global_south), ]
 
 # Count papers by global south status and citation status
 gs_cited <- sum(df_valid$collaboration_global_south == 1 & df_valid$cited_in_policy == 1)
 gs_uncited <- sum(df_valid$collaboration_global_south == 1 & df_valid$cited_in_policy == 0)
 non_gs_cited <- sum(df_valid$collaboration_global_south == 0 & df_valid$cited_in_policy == 1)
 non_gs_uncited <- sum(df_valid$collaboration_global_south == 0 & df_valid$cited_in_policy == 0)
 
 # Calculate ratio of uncited to cited for Global South
 gs_ratio <- gs_uncited / gs_cited
 
 # Calculate ratio of uncited to cited for non-Global South
 non_gs_ratio <- non_gs_uncited / non_gs_cited
 
 # Calculate how many times more likely Global South papers are to remain uncited
 relative_ratio <- gs_ratio / non_gs_ratio
 
 results <- list(
   gs_cited = gs_cited,
   gs_uncited = gs_uncited,
   non_gs_cited = non_gs_cited,
   non_gs_uncited = non_gs_uncited,
   gs_ratio = gs_ratio,
   non_gs_ratio = non_gs_ratio,
   relative_ratio = relative_ratio
 )
 
 return(results)
}

# Run the analysis
gs_results <- analyze_global_south(data)
print(gs_results)

# Print summary in desired format
cat("Papers with Global South authors:\n")
cat("  Cited:", gs_results$gs_cited, "\n")
cat("  Uncited:", gs_results$gs_uncited, "\n")
cat("  Ratio (uncited/cited):", round(gs_results$gs_ratio, 2), "\n\n")

cat("Papers without Global South authors:\n")
cat("  Cited:", gs_results$non_gs_cited, "\n")
cat("  Uncited:", gs_results$non_gs_uncited, "\n")
cat("  Ratio (uncited/cited):", round(gs_results$non_gs_ratio, 2), "\n\n")

cat("Global South papers are", round(gs_results$relative_ratio, 2), 
   "times as likely to remain uncited compared to non-Global South papers.\n")

# COMMAND ----------

# Calculate percentage of Global North only papers
analyze_global_south_percentages <- function(df) {
 # Remove rows with NA values
 df_valid <- df[!is.na(df$collaboration_global_south), ]
 
 # Total papers
 total_papers <- nrow(df_valid)
 
 # Global South papers
 gs_papers <- sum(df_valid$collaboration_global_south == 1)
 
 # Global North only papers
 gn_papers <- sum(df_valid$collaboration_global_south == 0)
 
 # Calculate percentages
 gs_percent <- gs_papers / total_papers * 100
 gn_percent <- gn_papers / total_papers * 100
 
 return(list(
   total_papers = total_papers,
   gs_papers = gs_papers,
   gn_papers = gn_papers,
   gs_percent = gs_percent,
   gn_percent = gn_percent
 ))
}

# Run the analysis
gs_percentages <- analyze_global_south_percentages(data)
print(gs_percentages)

# COMMAND ----------

# MAGIC %md
# MAGIC ##checked till here

# COMMAND ----------

# Function to analyze government collaboration patterns
analyze_govt_collab <- function(df) {
 # Remove rows with NA values for government_collaboration
 df_valid <- df[!is.na(df$collaboration_withGovt_authors), ]
 
 # Count papers by government collaboration status and citation status
 govt_cited <- sum(df_valid$collaboration_withGovt_authors == 1 & df_valid$cited_in_policy == 1)
 govt_uncited <- sum(df_valid$collaboration_withGovt_authors == 1 & df_valid$cited_in_policy == 0)
 non_govt_cited <- sum(df_valid$collaboration_withGovt_authors == 0 & df_valid$cited_in_policy == 1)
 non_govt_uncited <- sum(df_valid$collaboration_withGovt_authors == 0 & df_valid$cited_in_policy == 0)
 
 # Calculate ratios
 govt_ratio <- govt_cited / govt_uncited
 non_govt_ratio <- non_govt_cited / non_govt_uncited
 
 results <- list(
   govt_cited = govt_cited,
   govt_uncited = govt_uncited,
   non_govt_cited = non_govt_cited,
   non_govt_uncited = non_govt_uncited,
   govt_ratio = govt_ratio,
   non_govt_ratio = non_govt_ratio,
   govt_cited_pct = govt_cited / (govt_cited + govt_uncited) * 100,
   non_govt_cited_pct = non_govt_cited / (non_govt_cited + non_govt_uncited) * 100
 )
 
 return(results)
}

# Run the analysis
govt_results <- analyze_govt_collab(data)
print(govt_results)

# Print summary
cat("Government collaboration papers:\n")
cat("  Cited:", govt_results$govt_cited, "\n")
cat("  Uncited:", govt_results$govt_uncited, "\n")
cat("  Ratio (cited/uncited):", round(govt_results$govt_ratio, 2), "\n")
cat("  Percentage cited:", round(govt_results$govt_cited_pct, 1), "%\n\n")

cat("Non-government collaboration papers:\n")
cat("  Cited:", govt_results$non_govt_cited, "\n")
cat("  Uncited:", govt_results$non_govt_uncited, "\n")
cat("  Ratio (cited/uncited):", round(govt_results$non_govt_ratio, 2), "\n")
cat("  Percentage cited:", round(govt_results$non_govt_cited_pct, 1), "%\n")

# COMMAND ----------

analyze_govt_pairs <- function(df) {
 # Initialize counters
 results <- list(
   govt_cited_nongov_uncited = 0,
   nongov_cited_govt_uncited = 0,
   both_govt = 0,
   both_nongov = 0,
   invalid_pairs = 0
 )
 
 # Get unique pair IDs
 pair_ids <- unique(df$pair_id)
 
 for (pid in pair_ids) {
   pair_data <- df[df$pair_id == pid, ]
   
   # Skip if pair doesn't have exactly 2 rows or has NA values for govt_collaboration
   if (nrow(pair_data) != 2 || any(is.na(pair_data$collaboration_withGovt_authors))) {
     results$invalid_pairs <- results$invalid_pairs + 1
     next
   }
   
   cited <- pair_data[pair_data$cited_in_policy == 1, ]
   uncited <- pair_data[pair_data$cited_in_policy == 0, ]
   
   # Skip if we don't have one of each
   if (nrow(cited) != 1 || nrow(uncited) != 1) {
     results$invalid_pairs <- results$invalid_pairs + 1
     next
   }
   
   # Check government collaboration status
   if (cited$collaboration_withGovt_authors == 1 && uncited$collaboration_withGovt_authors == 0) {
     results$govt_cited_nongov_uncited <- results$govt_cited_nongov_uncited + 1
   } else if (cited$collaboration_withGovt_authors == 0 && uncited$collaboration_withGovt_authors == 1) {
     results$nongov_cited_govt_uncited <- results$nongov_cited_govt_uncited + 1
   } else if (cited$collaboration_withGovt_authors == 1 && uncited$collaboration_withGovt_authors == 1) {
     results$both_govt <- results$both_govt + 1
   } else {
     results$both_nongov <- results$both_nongov + 1
   }
 }
 
 return(results)
}

# Run the analysis
govt_pair_results <- analyze_govt_pairs(data)
print(govt_pair_results)

# Calculate percentages
total_valid_pairs <- sum(govt_pair_results$govt_cited_nongov_uncited, 
                        govt_pair_results$nongov_cited_govt_uncited,
                        govt_pair_results$both_govt,
                        govt_pair_results$both_nongov)

cat("Government collaboration in pairs:\n")
cat("  Govt paper cited, non-govt uncited:", govt_pair_results$govt_cited_nongov_uncited, 
   "(", round(govt_pair_results$govt_cited_nongov_uncited/total_valid_pairs*100, 1), "%)\n")
cat("  Non-govt paper cited, govt uncited:", govt_pair_results$nongov_cited_govt_uncited,
   "(", round(govt_pair_results$nongov_cited_govt_uncited/total_valid_pairs*100, 1), "%)\n")
cat("  Both papers govt collaboration:", govt_pair_results$both_govt,
   "(", round(govt_pair_results$both_govt/total_valid_pairs*100, 1), "%)\n")
cat("  Both papers non-govt:", govt_pair_results$both_nongov,
   "(", round(govt_pair_results$both_nongov/total_valid_pairs*100, 1), "%)\n")
cat("  Total valid pairs:", total_valid_pairs, "\n")

# COMMAND ----------

# Create a contingency table for government collaboration vs citation status
govt_matrix <- function(df) {
 # Remove rows with NA values for government_collaboration
 df_valid <- df[!is.na(df$collaboration_withGovt_authors), ]
 
 # Create 2x2 matrix
 matrix_data <- matrix(0, nrow = 2, ncol = 2)
 
 # Fill in the counts
 matrix_data[1,1] <- sum(df_valid$collaboration_withGovt_authors == 1 & df_valid$cited_in_policy == 1)
 matrix_data[1,2] <- sum(df_valid$collaboration_withGovt_authors == 1 & df_valid$cited_in_policy == 0)
 matrix_data[2,1] <- sum(df_valid$collaboration_withGovt_authors == 0 & df_valid$cited_in_policy == 1)
 matrix_data[2,2] <- sum(df_valid$collaboration_withGovt_authors == 0 & df_valid$cited_in_policy == 0)
 
 # Set row and column names
 rownames(matrix_data) <- c("Govt Collab", "Non-Govt")
 colnames(matrix_data) <- c("Cited", "Uncited")
 
 return(matrix_data)
}

# Run the analysis
result_matrix <- govt_matrix(data)
print(result_matrix)

# Print row and column percentages
cat("\nPercentage cited by collaboration type:\n")
cat("  Govt Collab:", round(result_matrix[1,1]/(result_matrix[1,1]+result_matrix[1,2])*100, 1), "%\n")
cat("  Non-Govt:", round(result_matrix[2,1]/(result_matrix[2,1]+result_matrix[2,2])*100, 1), "%\n")

# COMMAND ----------

analyze_govt_cited_pairs <- function(df) {
 # Initialize counter
 govt_cited_count <- 0
 nongov_cited_count <- 0
 total_valid_pairs <- 0
 
 # Get unique pair IDs
 pair_ids <- unique(df$pair_id)
 
 for (pid in pair_ids) {
   pair_data <- df[df$pair_id == pid, ]
   
   # Skip if pair doesn't have exactly 2 rows or has NA values
   if (nrow(pair_data) != 2 || any(is.na(pair_data$collaboration_withGovt_authors))) {
     next
   }
   
   cited <- pair_data[pair_data$cited_in_policy == 1, ]
   uncited <- pair_data[pair_data$cited_in_policy == 0, ]
   
   # Skip if we don't have one of each
   if (nrow(cited) != 1 || nrow(uncited) != 1) {
     next
   }
   
   # Count valid pairs
   total_valid_pairs <- total_valid_pairs + 1
   
   # Check which one is cited
   if (cited$collaboration_withGovt_authors == 1) {
     govt_cited_count <- govt_cited_count + 1
   } else {
     nongov_cited_count <- nongov_cited_count + 1
   }
 }
 
 return(list(
   govt_cited = govt_cited_count,
   nongov_cited = nongov_cited_count,
   total_pairs = total_valid_pairs,
   govt_cited_pct = govt_cited_count/total_valid_pairs*100,
   nongov_cited_pct = nongov_cited_count/total_valid_pairs*100
 ))
}

# Run the analysis
pair_citation_results <- analyze_govt_cited_pairs(data)
print(pair_citation_results)

# COMMAND ----------

analyze_govt_cited_in_mixed_pairs <- function(df) {
 govt_cited_count <- 0
 nongov_cited_count <- 0
 mixed_pairs_count <- 0
 
 pair_ids <- unique(df$pair_id)
 
 for (pid in pair_ids) {
   pair_data <- df[df$pair_id == pid, ]
   
   if (nrow(pair_data) != 2 || any(is.na(pair_data$collaboration_withGovt_authors))) {
     next
   }
   
   # Check if this is a mixed pair
   if (sum(pair_data$collaboration_withGovt_authors) == 1) {
     mixed_pairs_count <- mixed_pairs_count + 1
     
     cited <- pair_data[pair_data$cited_in_policy == 1, ]
     uncited <- pair_data[pair_data$cited_in_policy == 0, ]
     
     if (nrow(cited) != 1 || nrow(uncited) != 1) {
       next
     }
     
     if (cited$collaboration_withGovt_authors == 1) {
       govt_cited_count <- govt_cited_count + 1
     } else {
       nongov_cited_count <- nongov_cited_count + 1
     }
   }
 }
 
 return(list(
   mixed_pairs = mixed_pairs_count,
   govt_cited = govt_cited_count,
   nongov_cited = nongov_cited_count,
   govt_cited_pct = govt_cited_count/mixed_pairs_count*100
 ))
}

# Run the analysis
mixed_pair_results <- analyze_govt_cited_in_mixed_pairs(data)
print(mixed_pair_results)

# COMMAND ----------

analyze_govt_pair_types <- function(df) {
 # Initialize counters
 results <- list(
   both_govt = 0,
   both_nongov = 0,
   mixed_pairs = 0,
   invalid_pairs = 0
 )
 
 # Get unique pair IDs
 pair_ids <- unique(df$pair_id)
 
 for (pid in pair_ids) {
   pair_data <- df[df$pair_id == pid, ]
   
   # Skip if pair doesn't have exactly 2 rows or has NA values
   if (nrow(pair_data) != 2 || any(is.na(pair_data$collaboration_withGovt_authors))) {
     results$invalid_pairs <- results$invalid_pairs + 1
     next
   }
   
   # Check government collaboration status
   if (all(pair_data$collaboration_withGovt_authors == 1)) {
     results$both_govt <- results$both_govt + 1
   } else if (all(pair_data$collaboration_withGovt_authors == 0)) {
     results$both_nongov <- results$both_nongov + 1
   } else {
     results$mixed_pairs <- results$mixed_pairs + 1
   }
 }
 
 return(results)
}

# Run the analysis
govt_types <- analyze_govt_pair_types(data)
print(govt_types)

# Calculate percentages
total_valid_pairs <- sum(govt_types$both_govt, govt_types$both_nongov, govt_types$mixed_pairs)
cat("Pair types by government collaboration:\n")
cat("  Both government collaboration:", govt_types$both_govt, 
   "(", round(govt_types$both_govt/total_valid_pairs*100, 1), "%)\n")
cat("  Both non-government:", govt_types$both_nongov,
   "(", round(govt_types$both_nongov/total_valid_pairs*100, 1), "%)\n")
cat("  Mixed (one govt, one non-govt):", govt_types$mixed_pairs,
   "(", round(govt_types$mixed_pairs/total_valid_pairs*100, 1), "%)\n")
cat("  Total valid pairs:", total_valid_pairs, "\n")

# COMMAND ----------

# Function to count academic-only, government-only, and collaborative papers
count_paper_types <- function(df) {
 # Remove rows with NA values for government_collaboration
 df_valid <- df[!is.na(df$collaboration_withGovt_authors), ]
 
 # Count total papers
 total_papers <- nrow(df_valid)
 
 # Count government collaboration papers (academic-government)
 collab_papers <- sum(df_valid$collaboration_withGovt_authors == 1)
 
 # Count academic-only papers (assuming government_collaboration = 0 means academic-only)
 academic_only <- sum(df_valid$collaboration_withGovt_authors == 0)
 
 # Calculate percentages
 collab_percent <- collab_papers / total_papers * 100
 academic_percent <- academic_only / total_papers * 100
 
 return(list(
   total_papers = total_papers,
   academic_only = academic_only,
   govt_collab = collab_papers,
   academic_percent = academic_percent,
   collab_percent = collab_percent
 ))
}

# Run the analysis
paper_types <- count_paper_types(data)
print(paper_types)

# COMMAND ----------

# If you haven't already, load the survival package for clogit()
library(survival)

# 1. Inspect your columns
actual_cols <- colnames(data)
cat("Actual column names:", 
    paste(actual_cols[1:min(10, length(actual_cols))], collapse = ", "), 
    "...\n\n")

# 2. Specify the metrics you want to test
parameters <- c(
  "CiteScore", "academic_citations_adjusted", "max_h_index", 
  "collaboration_global_south", "collaboration_withGovt_authors", 
  "media_mentions", "team_past_policy_impact", "num_countries"
)

# 3. Prepare a results container
results <- data.frame(
  parameter      = character(),
  coefficient    = numeric(),
  p_value        = numeric(),
  concordance    = numeric(),
  n_observations = numeric(),
  stringsAsFactors = FALSE
)

# 4. Loop over each metric
for (param in parameters) {
  cat("\n---------------------------------------\n")
  cat("Processing parameter:", param, "\n")
  
  # Skip if the column doesn't exist
  if (!(param %in% names(data))) {
    cat("Error: Parameter", param, "not found in dataset\n")
    next
  }
  
  # Filter out rows with NA in the metric, the outcome, or the strata
  tmp <- subset(data,
                !is.na(data[[param]]) &
                !is.na(cited_in_policy) &
                !is.na(pair_id))
  cat("Number of complete cases:", nrow(tmp), "\n")
  
  # Skip if zero variance
  if (var(tmp[[param]], na.rm = TRUE) == 0) {
    cat("Error: Parameter", param, "has zero variance. Cannot scale.\n")
    next
  }
  
  # Standardize the metric
  param_z_col <- paste0(param, "_z")
  tmp[[param_z_col]] <- (tmp[[param]] - mean(tmp[[param]], na.rm = TRUE)) / 
                        sd(tmp[[param]], na.rm = TRUE)
  
  # Build the clogit formula
  formula_obj <- as.formula(
    paste0("cited_in_policy ~ ", param_z_col, " + strata(pair_id)")
  )
  
  # Fit the model and capture results
  tryCatch({
    model    <- clogit(formula_obj, data = tmp)
    sm       <- summary(model)
    coef_tbl <- sm$coefficients
    
    # Extract coefficient, p-value, and concordance
    coef_val <- coef_tbl[param_z_col,      "coef"]
    p_val    <- coef_tbl[param_z_col, "Pr(>|z|)"]
    conc     <- sm$concordance[1]  # only the concordance statistic
    
    # Print summary to console
    cat("\nModel summary for", param, ":\n")
    print(sm)
    
    # Store in results
    results <- rbind(results, data.frame(
      parameter      = param,
      coefficient    = coef_val,
      p_value        = p_val,
      concordance    = conc,
      n_observations = nrow(tmp),
      stringsAsFactors = FALSE
    ))
  }, error = function(e) {
    cat("Error in model for parameter", param, ":", e$message, "\n")
    results <- rbind(results, data.frame(
      parameter      = param,
      coefficient    = NA,
      p_value        = NA,
      concordance    = NA,
      n_observations = nrow(tmp),
      stringsAsFactors = FALSE
    ))
  })
}

# 5. Format & print the consolidated results
if (nrow(results) > 0) {
  # Nicely format p-values and add significance stars
  results$p_value_formatted <- ifelse(
    results$p_value < 0.001, "p < 0.001",
    ifelse(results$p_value < 0.01, "p < 0.01",
           ifelse(results$p_value < 0.05, "p < 0.05",
                  paste0("p = ", round(results$p_value, 3))))
  )
  results$significance <- ifelse(
    results$p_value < 0.001, "***",
    ifelse(results$p_value < 0.01, "**",
           ifelse(results$p_value < 0.05, "*", ""))
  )
  
  cat("\n==============================================\n")
  cat("SUMMARY OF RESULTS:\n")
  cat("==============================================\n\n")
  
  for (i in seq_len(nrow(results))) {
    cat(sprintf("Parameter: %s\n", results$parameter[i]))
    cat(sprintf("  Coefficient: %.3f\n", results$coefficient[i]))
    cat(sprintf("  Odds Ratio:  %.3f\n", exp(results$coefficient[i])))
    cat(sprintf("  P-value:     %s %s\n", 
                results$p_value_formatted[i], results$significance[i]))
    cat(sprintf("  Concordance: %.3f\n", results$concordance[i]))
    cat(sprintf("  Observations: %d\n\n", results$n_observations[i]))
  }
  
  # Optionally save to CSV
  write.csv(results, "clogit_regression_results.csv", row.names = FALSE)
  
} else {
  cat("\nNo valid results were produced. Please check your data structure.\n")
}

# COMMAND ----------

