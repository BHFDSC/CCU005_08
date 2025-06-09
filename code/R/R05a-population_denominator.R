
library(tidyverse)

cohort = read_rds(here::here("data", "cohort_cleaned.rds"))

study_dates = cohort %>% 
  summarise(
    min_stroke_date = first(min_stroke_date),
    max_stroke_date = first(max_stroke_date)
  )

monthly_periods = study_dates %>%
  mutate(month = map2(min_stroke_date, max_stroke_date, ~seq.Date(.x, .y, by = 'month'))) %>%
  unnest(month) %>%
  mutate(
    year = floor_date(month, unit = "year"),
    month = floor_date(month, unit = "month"),
    days_observed = days_in_month(month)
  ) %>% 
  select(-c(min_stroke_date, max_stroke_date))

annual_periods = monthly_periods %>% 
  group_by(year) %>% 
  summarise(days_observed = sum(days_observed))




population_denominator = read_csv(
  here::here("data", "reference_data", "england_population_estimates_by_sex_age_region_2023.csv"),
  show_col_types = FALSE
) %>% 
  janitor::clean_names() %>%
  select(-c(code, all_persons, all_males, all_females)) %>%
  rename(region = regions) %>% 
  mutate(across(-c(year, region), as.character)) %>%
  mutate(year = year %>% ymd(truncated = 2)) %>% 
  pivot_longer(
    cols = -c(year, region),
    values_to = "pop_denom_str",
    names_to = c("sex", "age"),
    names_pattern = "([mf])(\\d+)"
  )

population_denominator = population_denominator %>%
  mutate(
    pop_denom_str = case_when(
      pop_denom_str == "missing data" ~ NA_character_,
      TRUE ~ pop_denom_str
    ),
    pop_denom = pop_denom_str %>%
      str_remove_all(",") %>% 
      as.numeric()
  )


population_denominator = population_denominator  %>% 
  mutate(
    sex = sex %>% 
      factor() %>% 
      fct_recode(
        "Male" = "m",
        "Female" = "f"
      ) %>% 
      fct_relevel("Female"),
    age = age %>% as.numeric(),
    age_group = age %>% 
      cut(
        breaks = c(18, 60, 70, 80, 90, Inf),
        labels = c("18-59", "60-69", "70-79", "80-89", "90+"),
        include.lowest = TRUE, right = FALSE
      ) %>% 
      factor()
  ) %>% 
  filter(!is.na(age_group)) %>% 
  filter(year >= ymd("2020-01-01")) %>% 
  filter(! region == "Wales")

population_denominator = population_denominator  %>%
  mutate(
    region = region %>% 
      factor() %>% 
      fct_recode(
        "East of England" = "East"
      )
  ) %>% 
  group_by(year, region, age_group, sex) %>% 
  summarise(pop_denom = sum(pop_denom)) %>% 
  ungroup()

population_denominator_overall = population_denominator %>% 
  group_by(year, age_group, sex) %>% 
  summarise(pop_denom = sum(pop_denom))


eu_standard_population = read_csv(
  here::here("data", "reference_data", "european_standard_population.csv")) %>% 
  mutate(
    age_group = case_when(
      (age >= 18) & (age <= 59) ~ "18-59",
      (age >= 60) & (age <= 69) ~ "60-69",
      (age >= 70) & (age <= 79) ~ "70-79",
      (age >= 80) & (age <= 89) ~ "80-89",
      (age >= 90) ~ "90+",
    )
  ) %>%
  filter(!is.na(age_group)) %>% 
  group_by(age_group, sex) %>% 
  summarise(esp_weights = sum(population))


denominator_monthly_region = monthly_periods %>% 
  left_join(population_denominator, by = "year") %>% 
  full_join(eu_standard_population, by = c("age_group", "sex")) %>% 
  mutate(
    person_days = pop_denom * days_observed,
    person_years = person_days / 365.25,
  )

denominator_annual_region = annual_periods %>% 
  left_join(population_denominator, by = "year") %>% 
  full_join(eu_standard_population, by = c("age_group", "sex"))  %>% 
  mutate(
    person_days = pop_denom * days_observed,
    person_years = person_days / 365.25,
  )


denominator_monthly_overall = monthly_periods %>% 
  left_join(population_denominator_overall, by = "year") %>% 
  full_join(eu_standard_population, by = c("age_group", "sex")) %>% 
  mutate(
    person_days = pop_denom * days_observed,
    person_years = person_days / 365.25,
  )

denominator_annual_overall = annual_periods %>% 
  left_join(population_denominator_overall, by = "year") %>% 
  full_join(eu_standard_population, by = c("age_group", "sex"))  %>% 
  mutate(
    person_days = pop_denom * days_observed,
    person_years = person_days / 365.25,
  )


write_rds(
  denominator_monthly_region,
  here::here("data", "denominator_monthly_region.rds")
)

write_rds(
  denominator_annual_region,
  here::here("data", "denominator_annual_region.rds")
)

write_rds(
  denominator_monthly_overall,
  here::here("data", "denominator_monthly_overall.rds")
)

write_rds(
  denominator_annual_overall,
  here::here("data", "denominator_annual_overall.rds")
)
