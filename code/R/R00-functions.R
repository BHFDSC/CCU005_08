
# mykable: A default kable format
mykable = function(df, align = c("l", rep("r", 20)), ...){
  return(
    kableExtra::kable_styling(kableExtra::kable(df, align = align, row.names = FALSE, ...))
  )
}

# ff_remove_ref_custom: Remove reference levels but not label with "Total N (%)"
ff_remove_ref_custom = function (.data, only_binary = TRUE) 
{
  if (!any(names(.data) == "label")) 
    stop("finalfit function must include: add_dependent_label = FALSE")
  df.out = .data %>% dplyr::mutate(label = ifelse(label == 
                                                    "", NA, label)) %>% tidyr::fill(label) %>% dplyr::group_by(label)
  if (only_binary) {
    df.out = df.out %>%
      dplyr::filter(
        levels %in% c("Mean (SD)", "Median (IQR)") | dplyr::row_number() != 1 
        | dplyr::n() > 2 | label == "Total N (%)"
      )
  }
  else {
    df.out = df.out %>%
      dplyr::filter(
        levels %in% c("Mean (SD)", "Median (IQR)") | dplyr::row_number() != 1
        | label == "Total N (%)"
      )
  }
  df.out %>% as.data.frame() %>% rm_duplicate_labels()
}



# ff_round_counts: Round counts in finalfit summary table to nearest multiple 
ff_round_counts = function (.data, accuracy = 5, ignore = c("label", "levels", "p")){ 
  if (!any(names(.data) == "label")) 
    stop("summary_factorlist() must include: add_dependent_label = FALSE")
  df.out = .data %>%
    dplyr::mutate(label = dplyr::if_else(label == "", NA_character_, label)) %>% 
    tidyr::fill(label) %>%
    dplyr::group_by(label) %>% 
    dplyr::mutate(across(-dplyr::any_of(ignore), 
                         function(.){
                           value_count = as.numeric(stringr::str_extract(., "[:digit:]+")) %>% 
                             plyr::round_any(accuracy)
                           value_perc = value_count/sum(value_count)*100
                           
                           dplyr::case_when(
                             !levels %in% c("Mean (SD)", "Median (IQR)") ~ 
                               format_n_percent(value_count, value_perc, 1), 
                             TRUE ~ .)
                         })) %>%
    dplyr::mutate(label = dplyr::if_else(dplyr::row_number()==1, label, "")) %>% 
    dplyr::ungroup()
  class(df.out) = c("data.frame.ff", class(df.out))
  return(df.out)
}


# ff_round_counts: Round counts from finalfit::summary_factorlist output
ff_redact_counts = function (.data, n_redact = 10, ignore = c("label", "levels", "p")){ 
  if (!any(names(.data) == "label")) 
    stop("summary_factorlist() must include: add_dependent_label = FALSE")
  df.out = .data %>%
    dplyr::mutate(label = dplyr::if_else(label == "", NA_character_, label)) %>% 
    tidyr::fill(label) %>%
    dplyr::group_by(label) %>% 
    dplyr::mutate(across(-dplyr::any_of(ignore), 
                         function(.){
                           value_count = as.numeric(stringr::str_extract(., "[:digit:]+"))
                           value_readact = if_else(value_count < n_redact, NA_real_, value_count)
                           value_perc = value_readact/sum(value_readact, na.rm = TRUE)*100
                           
                           dplyr::case_when(
                             levels %in% c("Mean (SD)", "Median (IQR)") ~ .,
                             is.na(value_readact) ~ "[REDACTED]",
                             !levels %in% c("Mean (SD)", "Median (IQR)") ~ 
                               format_n_percent(value_readact, value_perc, 1), 
                             TRUE ~ .)
                           
                         })) %>%
    dplyr::mutate(label = dplyr::if_else(dplyr::row_number()==1, label, "")) %>% 
    dplyr::ungroup()
  class(df.out) = c("data.frame.ff", class(df.out))
  return(df.out)
}