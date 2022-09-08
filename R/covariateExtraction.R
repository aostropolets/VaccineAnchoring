# Covariate extraction for comparing baseline characteristics for patients anchored on a date and visit
# here, shown for day 0; for other windows modify accordingly

library(DatabaseConnector)
library(FeatureExtraction)

covariateSettings_0 <- createCovariateSettings(useDemographicsGender = TRUE,
                                               useDemographicsAgeGroup = TRUE,
                                               useDemographicsRace = TRUE,
                                               useDemographicsIndexYear = TRUE,
                                               useDemographicsIndexMonth = TRUE,
                                               useConditionOccurrenceShortTerm = TRUE,
                                               useDrugExposureShortTerm = TRUE,
                                               useProcedureOccurrenceShortTerm = TRUE,
                                               useMeasurementShortTerm = TRUE,
                                               useMeasurementRangeGroupShortTerm = TRUE,
                                               useObservationShortTerm = TRUE,
                                               useDeviceExposureShortTerm = TRUE,
                                               useCharlsonIndex = TRUE,
                                               useDcsi = TRUE,
                                               useChads2 = TRUE,
                                               useChads2Vasc = TRUE,
                                               useVisitCountShortTerm = TRUE,
                                               shortTermStartDays = 0,
                                               endDays = 0)

# input your connection details
covariateData_0 <- getDbCovariateData(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = resultsDatabaseSchema,
  cohortTable = "cohort",
  cohortId = c(1,2,3,4,5,6,7,8,9,10,11,12),
  covariateSettings = covariateSettings_0,
  aggregated = TRUE)


# standardized difference of means
std1_0 = computeStandardizedDifference(covariateData_0,covariateData_0,cohortId1 = 1,cohortId2 = 2)
std2_0 = computeStandardizedDifference(covariateData_0,covariateData_0,cohortId1 = 3,cohortId2 = 4)
std3_0 = computeStandardizedDifference(covariateData_0,covariateData_0,cohortId1 = 5,cohortId2 = 6)
std4_0 = computeStandardizedDifference(covariateData_0,covariateData_0,cohortId1 = 7,cohortId2 = 8)
std5_0 = computeStandardizedDifference(covariateData_0,covariateData_0,cohortId1 = 9,cohortId2 = 10)
std6_0 = computeStandardizedDifference(covariateData_0,covariateData_0,cohortId1 = 11,cohortId2 = 12)

std1_0 = std1_0%>%
  mutate(category = "covid_target_vs_date", day = "0-0")
std2_0 = std2_0%>%
  mutate(category = "flu_target_vs_date", day = "0-0")
std3_0 = std3_0%>%
  mutate(category = "covid_target_vs_visit", day = "0-0")
std4_0= std4_0%>%
  mutate(category = "flu_target_vs_visit", day = "0-0")
std5_0 = std5_0%>%
  mutate(category = "covid_target_same_visit", day = "0-0")
std6_0 = std6_0%>%
  mutate(category = "flu_target_same_visit", day = "0-0")

std_0 = rbind (std1_0,std2_0,std3_0,std4_0, std5_0, std6_0 )

