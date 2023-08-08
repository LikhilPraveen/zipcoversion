USE [INTEGRATION_PLUS_DB]
GO
/****** Object:  StoredProcedure [dbo].[usp_cotiviti_daily_claim_cds_insertion_impl]    Script Date: 7/28/2023 3:48:45 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author          : UST OFFSHORE
-- Create date     : 04/08/2022
-- Description     : To retrive the Claim Details from HRDW for the given calim fact key to load into CDS tables
-- Tables involved :
--                   ALL_CLAIM_FACT
--                   ALL_CLAIM_LINE_FACT
--                   ALL_ACCOUNT_HISTORY_FACT
--                   ALL_MEMBER_VERSION_HIST_FACT
--                   ALL_PRACTITIONER_HISTORY_FACT
--                   ALL_SUPPLIER_HISTORY_FACT
--                   CLAIM_FACT_TO_DIAGNOSIS
--                   CLAIM_FACT_TO_PROCEDURE_CODE
--                   DIAGNOSIS
--                   DIAGNOSIS_POA_INDICATOR_CODE
--                   DRG_HISTORY_FACT
--                   ID_TYPE_CODE
--                   MEMBER_OTHER_ID
--                   PAYMENT_FACT
--                   PAYMENT_FACT_TO_CLAIM_FACT
--                   POSTAL_ADDRESS
--                   PROVIDER_TAXONOMY
--                   TAX_ENTITY_HISTORY_FACT
--=============================================
--Version History		--Date             Changed by          Description
--1.0					04/08/2022     UST Offshore       Initial creation
--1.1					17/10/2022	   UST Offshore       Modified to include cotiviti 2.3 layout changes
--1.2					02/02/2023	   UST OFFSHORE		  PAYER_SHORT  field updated
--1.3					24/07/2023	   UST OFFSHORE		  Fix in Cotiviti OB (INTP-4769)
--1.4					07/25/2023     UST offshore       BILL_TYPE,RENDERING_TAXGROUP_ID fields updated
CREATE OR ALTER PROCEDURE [dbo].[usp_cotiviti_daily_claim_cds_insertion_impl]
(
 @pCLAIM_FACT_KEY NUMERIC(19)
,@pCLAIM_HCC_ID   VARCHAR(30),
 @pPAYER_SHORT   VARCHAR(5)
)
AS
 BEGIN

	SET ANSI_NULLS ON

	SET QUOTED_IDENTIFIER ON

	SET NOCOUNT ON

	SET ANSI_WARNINGS OFF
                               /*** VARIABLE DECLARATION ***/
  DECLARE
       @lv_return_code           INT
      ,@lv_Msg_Desc              VARCHAR(900)
      ,@ldt_Log_DTM              DATETIME
      ,@lv_Msg_Type              VARCHAR(50)
      ,@ln_claim_fact_key        NUMERIC(19)=NULL
      ,@ln_claim_start_date_key  NUMERIC(19)=NULL
      ,@lv_claim_type_name       VARCHAR(50)=''
      ,@ln_rendering_key         NUMERIC(19)=NULL
      ,@ln_location_key          NUMERIC(19)=NULL
      ,@ln_supplier_key          NUMERIC(19)=NULL
      ,@ln_payment_fact_key      NUMERIC(19)=NULL
	  ,@ln_referring_practitioner_key  NUMERIC(19 ) = NULL--2.3 changes
	  ,@lv_PAYER_SHORT             VARCHAR(5)
  BEGIN TRY

                             /*** CREATING TEMPERORY TABLES ***/

       -- DEBUG--  PRINT ' CREATING TEMPERORY TABLES Started : ' + CONVERT( varchar, Getdate(),121)

      DROP TABLE IF EXISTS #INSTITUTIONAL_CLAIM_DETAILS

      DROP TABLE IF EXISTS #PROFESSIONAL_CLAIM_DETAILS

      DROP TABLE IF EXISTS #CLAIM_DETAILS

      DROP TABLE IF EXISTS #MEMBER_DETAILS

      DROP TABLE IF EXISTS #PAYMENT

      CREATE TABLE #PAYMENT
      (
       CLAIM_FACT_KEY NUMERIC(19)
      ,CLAIM_PAID_DATE VARCHAR(255)
      )

      DROP TABLE IF EXISTS #CLAIM_DRG

      CREATE TABLE #CLAIM_DRG

      (
        CLAIM_FACT_KEY NUMERIC(19)
       ,SUB_DRG        VARCHAR(50)
       ,ALLOWED_DRG    VARCHAR(50)
      )

      DROP TABLE IF EXISTS #MEMBER_OTHER_ID

      CREATE TABLE #MEMBER_OTHER_ID
      (
        CLAIM_FACT_KEY            NUMERIC(19)
       ,MEMBER_HISTORY_FACT_KEY   NUMERIC(19)
       ,PATIENT_ID                VARCHAR(50)
      )

	  --2.3 changes start

	  DROP TABLE IF EXISTS #SUPPLIER_OTHER_ID

      CREATE TABLE #SUPPLIER_OTHER_ID
      (
        CLAIM_FACT_KEY            NUMERIC(19)
       ,SUPPLIER_HISTORY_FACT_KEY NUMERIC(19)
       ,MEDICARE_PROVIDER_ID      VARCHAR(80)
      )

	  --2.3 changes end

      DROP TABLE IF EXISTS #CLAIM_DIAGNOSIS

      CREATE TABLE #CLAIM_DIAGNOSIS
      (
        CLAIM_FACT_KEY                NUMERIC(19)
       ,DIAGNOSIS_CODE                VARCHAR(255)
       ,STANDARDIZED_DIAGNOSIS_CODE   VARCHAR(255)
       ,DIAGNOSIS_POA_INDICATOR_CODE  VARCHAR(50)
       ,SORT_ORDER                    VARCHAR(50)
       ,CLAIM_DIAGNOSIS_TYPE          VARCHAR(1)
       )

      DROP TABLE IF EXISTS #CLAIM_PROCEDURE

       CREATE TABLE #CLAIM_PROCEDURE
      (
        CLAIM_FACT_KEY           NUMERIC(19)
       ,PROCEDURE_CODE           VARCHAR(255)
       ,PROCEDURE_CODE_DATE      VARCHAR(255)
       ,SORT_ORDER               VARCHAR(50)
       )

      DROP TABLE IF EXISTS #BILLING_PROV_DETAILS

      CREATE TABLE #BILLING_PROV_DETAILS
      (
       CLAIM_FACT_KEY            NUMERIC(19)
      ,BILLING_PROVIDER_ID       VARCHAR(50)  DEFAULT ''
      ,BILLING_NPI               VARCHAR(50)  DEFAULT ''
      ,BILLING_SUBSPEC_ID        VARCHAR(50)  DEFAULT ''
      ,BILLING_TAXGROUP_ID       VARCHAR(50)  DEFAULT ''
      ,BILLING_ENTITY_TYPE       VARCHAR(10)  DEFAULT ''
      ,BILLING_LAST_NAME         VARCHAR(255) DEFAULT ''
      ,BILLING_FIRST_NAME        VARCHAR(255) DEFAULT ''
      ,BILLING_MIDDLE_NAME       VARCHAR(255) DEFAULT ''
      ,BILLING_NAME_SUFFIX       VARCHAR(255) DEFAULT ''
      ,BILLING_STREET_ADDRESS_1  VARCHAR(900) DEFAULT ''
      ,BILLING_STREET_ADDRESS_2  VARCHAR(900) DEFAULT ''
      ,BILLING_CITY              VARCHAR(50)  DEFAULT ''
      ,BILLING_STATE             VARCHAR(900) DEFAULT ''
      ,BILLING_COUNTRY           VARCHAR(900) DEFAULT ''
      ,BILLING_ZIP               VARCHAR(5)  DEFAULT ''
      ,BILLING_ZIP_PLUS_4        VARCHAR(4)   DEFAULT ''
      )

      DROP TABLE IF EXISTS #RENDERING_PROV_DETAILS

      CREATE TABLE #RENDERING_PROV_DETAILS
      (
       CLAIM_FACT_KEY              NUMERIC(19)
      ,RENDERING_PROVIDER_ID       VARCHAR(50)  DEFAULT ''
      ,RENDERING_NPI               VARCHAR(50)  DEFAULT ''
      ,RENDERING_SUBSPEC_ID        VARCHAR(50)  DEFAULT ''
      ,RENDERING_TAXGROUP_ID       VARCHAR(50)  DEFAULT ''
      ,RENDERING_ENTITY_TYPE       VARCHAR(10)  DEFAULT ''
      ,RENDERING_LAST_NAME         VARCHAR(255) DEFAULT ''
      ,RENDERING_FIRST_NAME        VARCHAR(255) DEFAULT ''
      ,RENDERING_MIDDLE_NAME       VARCHAR(255) DEFAULT ''
      ,RENDERING_NAME_SUFFIX       VARCHAR(255) DEFAULT ''
      ,RENDERING_STREET_ADDRESS_1  VARCHAR(900) DEFAULT ''
      ,RENDERING_STREET_ADDRESS_2  VARCHAR(900) DEFAULT ''
      ,RENDERING_CITY              VARCHAR(50)  DEFAULT ''
      ,RENDERING_STATE             VARCHAR(900) DEFAULT ''
      ,RENDERING_COUNTRY           VARCHAR(900) DEFAULT ''
      ,RENDERING_ZIP               VARCHAR(5)  DEFAULT ''
      ,RENDERING_ZIP_PLUS_4        VARCHAR(4)   DEFAULT ''
      )


      DROP TABLE IF EXISTS #CLAIM_AMOUNTS

       CREATE TABLE #CLAIM_AMOUNTS
      (
         CLAIM_FACT_KEY           NUMERIC(19)
        ,CLAIM_BILLED_AMOUNT      NUMERIC(19,2)
        ,CLAIM_NONCOVERED_AMOUNT  NUMERIC(19,2)
        ,CLAIM_ALLOWED_AMOUNT     NUMERIC(19,2)
        ,CLAIM_PAID_AMOUNT        NUMERIC(19,2)
		,CLAIM_COINSURANCE_AMOUNT NUMERIC(19,2)--2.3 changes
		,CLAIM_COPAY_AMOUNT		  NUMERIC(19,2)--2.3 changes
		,CLAIM_DEDUCTIBLE_AMOUNT  NUMERIC(19,2)--2.3 changes
		,CLAIM_PATIENT_LIABILITY_AMOUNT NUMERIC(19,2)--2.3 changes

      )

       -- DEBUG--  PRINT ' CREATING TEMPERORY TABLES Completed : ' + CONVERT( varchar, Getdate(),121)

                               /*** VARIABLE INITIALIZATION ***/

       -- DEBUG--  PRINT 'VARIABLE INITIALIZATION Started : ' + CONVERT( varchar, Getdate(),121)

      SET @ln_claim_fact_key=@pCLAIM_FACT_KEY
	  SET @lv_PAYER_SHORT  = @pPAYER_SHORT

       -- DEBUG--  PRINT 'VARIABLE INITIALIZATION Completed : ' + CONVERT( varchar, Getdate(),121)

                               /*** GETTING CLAIM'S INNFORMATION ***/
       -- DEBUG--  PRINT ' GETTING CLAIM INNFORMATION Started : ' + CONVERT( varchar, Getdate(),121)

      SELECT
           ACF.CLAIM_TYPE_NAME
          , @lv_PAYER_SHORT                         AS PAYER_SHORT
          ,LEFT(ACF.PATIENT_ACCOUNT_NUMBER,24) AS PATIENT_CONTROL_NUMBER
          ,LEFT(ACF.CLAIM_HCC_ID,25) AS CLAIM_ID
          ,LEFT(ACF.TYPE_OF_BILL_CODE,4) AS BILL_TYPE
          ,ACF.DISCHARGE_STATUS_CODE AS DISCHARGE_STATUS
          ,LEFT(ACF.PRINCIPAL_PROCEDURE_CODE,7) AS PRINCIPAL_PROCEDURE
          ,CONVERT(VARCHAR,PROC_DD.DATE_VALUE,112) AS PRINCIPAL_PROCEDURE_DATE
          ,LEFT(ACF.BENEFIT_ASSIGNMENT,1) AS ASSIGNMENT_OF_BENEFITS
          --If the claim is adjusted, gettting the previous claim's ID as original claim id
          ,CASE WHEN ACF.IS_ADJUSTED='Y' THEN ISNULL(LEFT(PREVACF.CLAIM_HCC_ID,25),'')
                ELSE ''
           END AS CLAIM_ID_ORIG
          ,LEFT(STAND_DIAG.STANDARDIZED_DIAGNOSIS_CODE,7) AS PRINCIPAL_DIAGNOSIS
		  ,CASE
                WHEN DPIC.DIAGNOSIS_POA_INDICATOR_CODE IN ('N/S','N/A')
                   THEN ''
                   ELSE LEFT(DPIC.DIAGNOSIS_POA_INDICATOR_CODE,1)
           END AS PRINCIPAL_DIAGNOSIS_POA
          ,LEFT(ADMIT_STAND_DIAG.STANDARDIZED_DIAGNOSIS_CODE,7) AS ADMITTING_DIAGNOSIS
          ,CONVERT(VARCHAR,START_DD.DATE_VALUE,112) AS CLAIM_DOS_FROM
          ,CONVERT(VARCHAR,END_DD.DATE_VALUE,112) AS CLAIM_DOS_TO
          ,CONVERT(VARCHAR,ADMISSION_DD.DATE_VALUE,112) AS ADMIT_DATE
          ,CONVERT(VARCHAR,DISCHARGE_DD.DATE_VALUE,112) AS DISCHARGE_DATE
          ,CONVERT(VARCHAR,RECEIPT_DD.DATE_VALUE,112) AS DATE_RECEIVED_CLIENT
          ,'Y' AS CV_US_ONLY_YN
          ,ACF.CLAIM_FACT_KEY
          ,ACF.SUBMITTED_DRG_KEY
          ,ACF.CALCULATED_DRG_KEY
          ,ACF.MEMBER_KEY
          ,ACF.SUPPLIER_KEY
          ,ACF.LOCATION_KEY
          ,ACF.ATTENDING_PRACTITIONER_KEY
		  ,ACF.ENDORSEMENT_EFF_TIME
		  ,ACF.IS_ADJUSTED AS IS_ADJUSTED_CLAIM
		  ,ACF.IS_VOIDED
		  ,ACF.STATEMENT_START_DATE_KEY
		  ,LEFT(COALESCE(MHIF.MBI, MHIF.HICN),25) AS SUB_MEDICARE_ID --2.3 changes
		  ,ACF.LENGTH_OF_STAY AS LENGTH_OF_STAY--2.3 changes
		  ,ACF.REFERRING_PRACTITIONER_KEY--2.3 changes
		  ,MOST_RECENT_DD.DATE_VALUE AS DATE_ADJUDICATED --V1.3
      INTO #CLAIM_DETAILS
      FROM HRDW_REPLICA.PAYOR_DW.ALL_CLAIM_FACT ACF
      LEFT JOIN HRDW_REPLICA.PAYOR_DW.ALL_CLAIM_FACT PREVACF
      ON ACF.PREVIOUS_FINAL_CLAIM_FACT_KEY=PREVACF.CLAIM_FACT_KEY ----If the claim is adjusted, gettting the previous claim's ID as original claim id
	  AND ISNULL(UPPER(PREVACF.DELETED_FLAG), '') <> 'Y'
      LEFT JOIN HRDW_REPLICA.PAYOR_DW.DIAGNOSIS STAND_DIAG
      ON ACF.PRIMARY_DIAGNOSIS_CODE=STAND_DIAG.DIAGNOSIS_CODE
	  AND ISNULL(UPPER(STAND_DIAG.DELETED_FLAG), '') <> 'Y'
      LEFT JOIN HRDW_REPLICA.PAYOR_DW.DIAGNOSIS_POA_INDICATOR_CODE DPIC
      ON ACF.PRIMARY_DIAGNOSIS_POA_IND_KEY=DPIC.DIAGNOSIS_POA_INDICATOR_KEY
	  AND ISNULL(UPPER(DPIC.DELETED_FLAG), '') <> 'Y'
      LEFT JOIN HRDW_REPLICA.PAYOR_DW.DIAGNOSIS ADMIT_STAND_DIAG
      ON ACF.ADMIT_DIAGNOSIS_CODE=ADMIT_STAND_DIAG.DIAGNOSIS_CODE
	  AND ISNULL(UPPER(ADMIT_STAND_DIAG.DELETED_FLAG), '') <> 'Y'
      LEFT JOIN HRDW_REPLICA.PAYOR_DW.DATE_DIMENSION PROC_DD --Principal procedure date
      ON ACF.PRINCIPAL_PROCEDURE_DATE_KEY=PROC_DD.DATE_KEY
	  AND ISNULL(UPPER(PROC_DD.DELETED_FLAG), '') <> 'Y'
      LEFT JOIN HRDW_REPLICA.PAYOR_DW.DATE_DIMENSION START_DD --Claim start date
      ON ACF.STATEMENT_START_DATE_KEY=START_DD.DATE_KEY
	  AND ISNULL(UPPER(START_DD.DELETED_FLAG), '') <> 'Y'
      LEFT JOIN HRDW_REPLICA.PAYOR_DW.DATE_DIMENSION END_DD --Claim end date
      ON ACF.STATEMENT_END_DATE_KEY=END_DD.DATE_KEY
	  AND ISNULL(UPPER(END_DD.DELETED_FLAG), '') <> 'Y'
      LEFT JOIN HRDW_REPLICA.PAYOR_DW.DATE_DIMENSION ADMISSION_DD --Admission date
      ON ACF.ADMISSION_DATE_KEY=ADMISSION_DD.DATE_KEY
	  AND ISNULL(UPPER(ADMISSION_DD.DELETED_FLAG), '') <> 'Y'
      LEFT JOIN HRDW_REPLICA.PAYOR_DW.DATE_DIMENSION DISCHARGE_DD --Discharge date
      ON ACF.DISCHARGE_DATE_KEY=DISCHARGE_DD.DATE_KEY
	  AND ISNULL(UPPER(DISCHARGE_DD.DELETED_FLAG), '') <> 'Y'
      LEFT JOIN HRDW_REPLICA.PAYOR_DW.DATE_DIMENSION RECEIPT_DD --Claim received date
      ON ACF.RECEIPT_DATE_KEY=RECEIPT_DD.DATE_KEY
	  AND ISNULL(UPPER(RECEIPT_DD.DELETED_FLAG), '') <> 'Y'
	  LEFT JOIN HRDW_REPLICA.PAYOR_DW.MEDICARE_HICN_INFO_FACT MHIF --SUB_MEDICARE_ID 2.3 changes
	  ON ACF.MEMBER_KEY = MHIF.MEMBER_KEY
	  LEFT JOIN HRDW_REPLICA.PAYOR_DW.DATE_DIMENSION MOST_RECENT_DD --MOST_RECENT_PROCESS_DATE_KEY V1.3
	  ON ACF.MOST_RECENT_PROCESS_DATE_KEY = MOST_RECENT_DD.DATE_KEY
	  AND ISNULL(UPPER(MHIF.DELETED_FLAG), '') <> 'Y'
      WHERE
        ACF.CLAIM_FACT_KEY=@ln_claim_fact_key
		AND ISNULL(UPPER(ACF.DELETED_FLAG), '') <> 'Y'

       -- DEBUG--  PRINT ' GETTING CLAIM INNFORMATION Completed : ' + CONVERT( varchar, Getdate(),121)

	  SELECT
				@ln_claim_start_date_key= STATEMENT_START_DATE_KEY,
				@ln_referring_practitioner_key =  REFERRING_PRACTITIONER_KEY--2.3 changes
      FROM #CLAIM_DETAILS



                               /*** GETTING MEMBER'S INFORMATION ***/

       -- DEBUG--  PRINT ' GETTING MEMBER INFORMATION Started : ' + CONVERT( varchar, Getdate(),121)

      SELECT
           CLAIM.CLAIM_FACT_KEY
          ,AMHF.MEMBER_HISTORY_FACT_KEY
          ,LEFT(AMHF.SUBSCRIPTION_HCC_ID,25) AS SUB_ID
          ,AMHF.MEMBER_GENDER_CODE AS PATIENT_GENDER_ID
          ,REPLACE(AMHF.TAX_ID,'-','') AS PATIENT_SSN --In HRP, Member's SSN mapped to TAX_ID at backend
          ,LEFT(AMHF.MEMBER_LAST_NAME,60) AS PATIENT_LAST_NAME
          ,LEFT(AMHF.MEMBER_FIRST_NAME,35) AS PATIENT_FIRST_NAME
          ,LEFT(AMHF.MEMBER_MIDDLE_NAME,25) AS PATIENT_MIDDLE_NAME
          ,LEFT(AMHF.MEMBER_NAME_SUFFIX,10) AS PATIENT_NAME_SUFFIX
          ,LEFT(AMHF.SUBSCRIBER_LAST_NAME,60) AS SUB_LAST_NAME
          ,LEFT(AMHF.SUBSCRIBER_FIRST_NAME,35) AS SUB_FIRST_NAME
          ,LEFT(AMHF.SUBSCRIBER_MIDDLE_NAME,25) AS SUB_MIDDLE_NAME
          ,LEFT(AMHF.SUBSCRIBER_NAME_SUFFIX,10) AS SUB_NAME_SUFFIX
          ,LEFT(AAHF.ACCOUNT_HCC_ID,15) AS GROUP_ID
		  ,LEFT(AAHF.ACCOUNT_NAME,60) AS GROUP_NAME
          ,LEFT(ADDR.ADDRESS_LINE,55) AS PATIENT_ADDRESS_1
          ,LEFT(ADDR.ADDRESS_LINE_2,55) AS  PATIENT_ADDRESS_2
          ,LEFT(ADDR.CITY_NAME,30) AS  PATIENT_CITY
          ,LEFT(ADDR.STATE_CODE,2) AS  PATIENT_STATE
          ,LEFT(ADDR.ZIP_CODE,5) AS PATIENT_ZIP
          ,ADDR.ZIP_4_CODE AS PATIENT_ZIP_PLUS_4
          ,ADDR.COUNTRY_CODE AS PATIENT_COUNTRY
          ,CONVERT(VARCHAR,BIRTH_DD.DATE_VALUE,112) AS PATIENT_DOB
          ,CONVERT(VARCHAR,DEATH_DD.DATE_VALUE,112) AS PATIENT_DEATH_DATE
          ,'18' AS RELATIONSHIP_TO_SUB
      INTO #MEMBER_DETAILS
      FROM #CLAIM_DETAILS CLAIM
      INNER JOIN HRDW_REPLICA.PAYOR_DW.ALL_MEMBER_VERSION_HIST_FACT AMHF
      ON CLAIM.MEMBER_KEY = AMHF.MEMBER_KEY
      LEFT JOIN HRDW_REPLICA.PAYOR_DW.ALL_ACCOUNT_HISTORY_FACT AAHF
      ON AMHF.ACCOUNT_KEY = AAHF.ACCOUNT_KEY
	  AND AAHF.VERSION_EFF_DATE_KEY <=  @ln_claim_start_date_key
	  AND AAHF.VERSION_EXP_DATE_KEY >   @ln_claim_start_date_key
	  AND UPPER(AAHF.ACCOUNT_STATUS) NOT IN ('R','V') --Record should not be in Workbasket (Repair,Review)
	  AND ISNULL(UPPER(AAHF.DELETED_FLAG), '') <> 'Y'
      LEFT JOIN HRDW_REPLICA.PAYOR_DW.POSTAL_ADDRESS ADDR
      ON AMHF.MEMBER_MAILING_ADDRESS_KEY=ADDR.POSTAL_ADDRESS_KEY
	  AND ISNULL(UPPER(ADDR.DELETED_FLAG), '') <> 'Y'
      LEFT JOIN HRDW_REPLICA.PAYOR_DW.DATE_DIMENSION BIRTH_DD --Member's DOB
      ON AMHF.MEMBER_BIRTH_DATE_KEY=BIRTH_DD.DATE_KEY
	  AND ISNULL(UPPER(BIRTH_DD.DELETED_FLAG), '') <> 'Y'
      LEFT JOIN HRDW_REPLICA.PAYOR_DW.DATE_DIMENSION DEATH_DD --Member's death date
      ON AMHF.MEMBER_DEATH_DATE_KEY=DEATH_DD.DATE_KEY
	  AND ISNULL(UPPER(DEATH_DD.DELETED_FLAG), '') <> 'Y'
      WHERE
          AMHF.ENDOR_EXP_DATE > GETDATE() --OR ISNULL(AMHF.ENDOR_EXP_DATE,'')=''
      AND AMHF.VERSION_EFF_DATE_KEY <=  @ln_claim_start_date_key
      AND AMHF.VERSION_EXP_DATE_KEY >   @ln_claim_start_date_key
	  AND UPPER(AMHF.MEMBER_STATUS) NOT IN ('R','V') --Record should not be in Workbasket (Repair,Review)
      AND ISNULL(UPPER(AMHF.DELETED_FLAG), '') <> 'Y'

       -- DEBUG--  PRINT ' GETTING MEMBER INFORMATION Completed : ' + CONVERT( varchar, Getdate(),121)

                               /*** GETTING MEMBER's OTHER ID (MEDICAID ID) ***/

       -- DEBUG--  PRINT ' GETTING MEMBER MDICAID ID Started : ' + CONVERT( varchar, Getdate(),121)

      INSERT INTO #MEMBER_OTHER_ID
      SELECT
            MEMBER.CLAIM_FACT_KEY
           ,MEMBER.MEMBER_HISTORY_FACT_KEY
           ,LEFT(MOI.IDENTIFICATION_NUMBER,20) AS PATIENT_ID
      FROM #MEMBER_DETAILS MEMBER
      INNER JOIN HRDW_REPLICA.PAYOR_DW.MEMBER_OTHER_ID MOI
      ON MEMBER.MEMBER_HISTORY_FACT_KEY = MOI.MEMBER_HISTORY_FACT_KEY
      INNER JOIN HRDW_REPLICA.PAYOR_DW.ID_TYPE_CODE ITC
      ON MOI.ID_TYPE_KEY = ITC.ID_TYPE_KEY
      WHERE
		  MOI.EFFECTIVE_START_DATE_KEY <=  @ln_claim_start_date_key --To get the id which is effective between the claim's date of service
	  AND MOI.EFFECTIVE_END_DATE_KEY   >   @ln_claim_start_date_key --To get the id which is effective between the claim's date of service
	  AND ISNULL(UPPER(MOI.DELETED_FLAG), '') <> 'Y'
	  AND ITC.ID_TYPE_CODE='20' --To get the medicaid id
	  AND ISNULL(UPPER(ITC.DELETED_FLAG), '') <> 'Y'

       -- DEBUG--  PRINT ' GETTING MEMBER MDICAID ID Completed : ' + CONVERT( varchar, Getdate(),121)

                               /*** GETTING CLAIM'S PAYMENT INFORMATION ***/

       -- DEBUG--  PRINT ' GETTING CLAIM PAYMENT INFORMATION Started : ' + CONVERT( varchar, Getdate(),121)
       --2.3 changes start
	  INSERT INTO #SUPPLIER_OTHER_ID
      SELECT
	  MEDPROVID.CLAIM_FACT_KEY
	  ,MEDPROVID.SUPPLIER_HISTORY_FACT_KEY
	  ,MEDPROVID.MEDICARE_PROVIDER_ID
	  FROM
	  (
	  SELECT
            CLAIM.CLAIM_FACT_KEY
           ,ASHF1.SUPPLIER_HISTORY_FACT_KEY
           ,SOI.IDENTIFICATION_NUMBER AS MEDICARE_PROVIDER_ID
		   ,ROW_NUMBER() OVER(PARTITION BY SOI.SUPPLIER_HISTORY_FACT_KEY
                            ORDER BY SOI.SUPPLIER_OTHER_ID_KEY DESC) AS RN
      FROM #CLAIM_DETAILS CLAIM
	  INNER JOIN HRDW_REPLICA.PAYOR_DW.ALL_SUPPLIER_HISTORY_FACT ASHF1
	  ON CLAIM.SUPPLIER_KEY = ASHF1.SUPPLIER_KEY
      INNER JOIN HRDW_REPLICA.PAYOR_DW.SUPPLIER_OTHER_ID SOI
      ON ASHF1.SUPPLIER_HISTORY_FACT_KEY = SOI.SUPPLIER_HISTORY_FACT_KEY
      INNER JOIN HRDW_REPLICA.PAYOR_DW.ID_TYPE_CODE ITC
      ON SOI.ID_TYPE_KEY = ITC.ID_TYPE_KEY
      WHERE
		  SOI.EFFECTIVE_START_DATE_KEY <=  @ln_claim_start_date_key --To get the id which is effective between the claim's date of service
	  AND SOI.EFFECTIVE_END_DATE_KEY   >   @ln_claim_start_date_key --To get the id which is effective between the claim's date of service
	  AND ISNULL(UPPER(SOI.DELETED_FLAG), '') <> 'Y'
	  AND ITC.ID_TYPE_CODE='1C' --To get the medicare provider id
	  AND ISNULL(UPPER(ITC.DELETED_FLAG), '') <> 'Y'
	  AND ASHF1.VERSION_EFF_DATE_KEY <=  @ln_claim_start_date_key
	  AND ASHF1.VERSION_EXP_DATE_KEY >   @ln_claim_start_date_key
	  AND UPPER(ASHF1.SUPPLIER_STATUS) NOT IN ('R','V') --Record should not be in Workbasket (Repair,Review)
	  AND ISNULL(UPPER(ASHF1.DELETED_FLAG), '') <> 'Y'
	  ) MEDPROVID
	  WHERE RN=1
	  --2.3 changes end

      INSERT INTO #PAYMENT
      SELECT
          TOP 1 PFTCF.CLAIM_FACT_KEY --Getting the latest one if multiple payment facts available for a claim
               ,CONVERT(VARCHAR,DD_PAY.DATE_VALUE,112) AS CLAIM_PAID_DATE -- Claim paid date
      FROM HRDW_REPLICA.PAYOR_DW.PAYMENT_FACT_TO_CLAIM_FACT PFTCF
      LEFT JOIN HRDW_REPLICA.PAYOR_DW.PAYMENT_FACT PF
      ON PFTCF.PAYMENT_FACT_KEY=PF.PAYMENT_FACT_KEY
	  AND PF.PAYMENT_STATUS_CODE IN ('3','7') --Issued and Not issued
	  AND ISNULL(UPPER(PF.DELETED_FLAG), '') <> 'Y'
      LEFT JOIN HRDW_REPLICA.PAYOR_DW.DATE_DIMENSION DD_PAY
      ON PF.PAYMENT_DATE_KEY=DD_PAY.DATE_KEY
	  AND ISNULL(UPPER(DD_PAY.DELETED_FLAG), '') <> 'Y'
      WHERE
        PFTCF.CLAIM_FACT_KEY=@ln_claim_fact_key
		AND ISNULL(UPPER(PFTCF.DELETED_FLAG), '') <> 'Y'
      ORDER BY PFTCF.PAYMENT_FACT_KEY DESC

      -- DEBUG--  PRINT ' GETTING CLAIM PAYMENT INFORMATION Completed : ' + CONVERT( varchar, Getdate(),121)

                               /*** GETTING CLAIM'S DRG INFORMATION ***/

       -- DEBUG--  PRINT ' GETTING CLAIM DRG INFORMATION Started : ' + CONVERT( varchar, Getdate(),121)

      INSERT INTO #CLAIM_DRG
      SELECT
           CLAIM.CLAIM_FACT_KEY
          ,SDHF.DRG_CODE
          ,CDHF.DRG_CODE
      FROM #CLAIM_DETAILS CLAIM
      LEFT JOIN HRDW_REPLICA.PAYOR_DW.DRG_HISTORY_FACT SDHF
      ON CLAIM.SUBMITTED_DRG_KEY=SDHF.DRG_KEY
	  AND SDHF.VERSION_EFF_DATE_KEY <=  @ln_claim_start_date_key
      AND SDHF.VERSION_EXP_DATE_KEY >   @ln_claim_start_date_key
	  AND UPPER(SDHF.DRG_STATUS) NOT IN ('R','V') --Record should not be in Workbasket (Repair,Review)
	  AND  ISNULL(UPPER(SDHF.DELETED_FLAG), '') <> 'Y'
      LEFT JOIN HRDW_REPLICA.PAYOR_DW.DRG_HISTORY_FACT CDHF
      ON CLAIM.CALCULATED_DRG_KEY=CDHF.DRG_KEY
	  AND CDHF.VERSION_EFF_DATE_KEY <=  @ln_claim_start_date_key
      AND CDHF.VERSION_EXP_DATE_KEY >   @ln_claim_start_date_key
	  AND UPPER(CDHF.DRG_STATUS) NOT IN ('R','V') --Record should not be in Workbasket (Repair,Review)
	  AND  ISNULL(UPPER(CDHF.DELETED_FLAG), '') <> 'Y'
      WHERE
		 ISNULL(SDHF.DRG_CODE,'') <> ''
	  OR ISNULL(CDHF.DRG_CODE,'') <> ''

       -- DEBUG--  PRINT ' GETTING CLAIM DRG INFORMATION Completed : ' + CONVERT( varchar, Getdate(),121)

                               /*** GETTING CLAIM'S DIAGNOSIS INFORMATION ***/

       -- DEBUG--  PRINT ' GETTING CLAIM DIAGNOSIS INFORMATION Started : ' + CONVERT( varchar, Getdate(),121)

      INSERT INTO #CLAIM_DIAGNOSIS
      SELECT
           CFTD.CLAIM_FACT_KEY
          ,CFTD.DIAGNOSIS_CODE
          ,LEFT(STAND_DIAG.STANDARDIZED_DIAGNOSIS_CODE,7)
          ,CASE
                WHEN DPIC.DIAGNOSIS_POA_INDICATOR_CODE IN ('N/S','N/A')
                   THEN ''
                   ELSE LEFT(DPIC.DIAGNOSIS_POA_INDICATOR_CODE,1)
          END AS DIAGNOSIS_POA_INDICATOR_CODE
          ,CFTD.SORT_ORDER
          ,CFTD.CLAIM_DIAGNOSIS_TYPE
      FROM #CLAIM_DETAILS CLAIM
      INNER JOIN HRDW_REPLICA.PAYOR_DW.CLAIM_FACT_TO_DIAGNOSIS CFTD
      ON CLAIM.CLAIM_FACT_KEY=CFTD.CLAIM_FACT_KEY
      INNER JOIN HRDW_REPLICA.PAYOR_DW.DIAGNOSIS STAND_DIAG
      ON CFTD.DIAGNOSIS_CODE=STAND_DIAG.DIAGNOSIS_CODE
	  AND ISNULL(UPPER(STAND_DIAG.DELETED_FLAG), '') <> 'Y'
      LEFT JOIN HRDW_REPLICA.PAYOR_DW.DIAGNOSIS_POA_INDICATOR_CODE DPIC
      ON CFTD.POA_INDICATOR=DPIC.DIAGNOSIS_POA_INDICATOR_KEY
	  AND ISNULL(UPPER(DPIC.DELETED_FLAG), '') <> 'Y'
	  WHERE
		  ISNULL(UPPER(CFTD.DELETED_FLAG), '') <> 'Y'


       -- DEBUG--  PRINT ' GETTING CLAIM DIAGNOSIS INFORMATION Completed : ' + CONVERT( varchar, Getdate(),121)

                               /*** GETTING CLAIM'S PROCEDURE INFORMATION ***/

       -- DEBUG--  PRINT ' GETTING CLAIM PROCEDURE INFORMATION Started : ' + CONVERT( varchar, Getdate(),121)

      INSERT INTO #CLAIM_PROCEDURE
      SELECT
           CFTP.CLAIM_FACT_KEY
          ,LEFT(CFTP.PROCEDURE_CODE,7)
          ,CONVERT(VARCHAR,ISNULL(DD.DATE_VALUE,''),112)
          ,CFTP.SORT_ORDER
      FROM #CLAIM_DETAILS CLAIM
      INNER JOIN HRDW_REPLICA.PAYOR_DW.CLAIM_FACT_TO_PROCEDURE_CODE CFTP
      ON CLAIM.CLAIM_FACT_KEY=CFTP.CLAIM_FACT_KEY
      LEFT JOIN HRDW_REPLICA.PAYOR_DW.DATE_DIMENSION DD
      ON CFTP.PROCEDURE_DATE_KEY = DD.DATE_KEY
	  AND ISNULL(UPPER(DD.DELETED_FLAG), '') <> 'Y'
	  WHERE
	      ISNULL(UPPER(CFTP.DELETED_FLAG), '') <> 'Y'


        -- DEBUG--  PRINT ' GETTING CLAIM PROCEDURE INFORMATION Completed : ' + CONVERT( varchar, Getdate(),121)

                               /*** GETTING CLAIM'S BILLING PROVIDER INFORMATION  ***/

       -- DEBUG--  PRINT ' GETTING CLAIM BILLING PROVIDER INFORMATION Started : ' + CONVERT( varchar, Getdate(),121)

      SELECT @ln_supplier_key=SUPPLIER_KEY
      FROM #CLAIM_DETAILS


      INSERT INTO #BILLING_PROV_DETAILS
            SELECT
                 @ln_claim_fact_key
                ,LEFT(ASHF.SUPPLIER_HCC_ID,25)
                ,LEFT(ASHF.SUPPLIER_NPI,10)
                ,LEFT(TAX.PROVIDER_TAXONOMY_CODE,10)
                ,REPLACE(TEHF.TAX_ID,'-','') AS TAX_ID
                ,'P'
                ,LEFT(ASHF.SUPPLIER_NAME,60)
                ,''
                ,''
                ,''
                ,LEFT(ADDR.ADDRESS_LINE,55)
                ,LEFT(ADDR.ADDRESS_LINE_2,55)
                ,LEFT(ADDR.CITY_NAME,30)
                ,LEFT(ADDR.STATE_CODE,2)
                ,ADDR.COUNTRY_CODE
                ,LEFT(ADDR.ZIP_CODE,5) AS ZIP_CODE
                ,ADDR.ZIP_4_CODE
            FROM HRDW_REPLICA.PAYOR_DW.ALL_SUPPLIER_HISTORY_FACT ASHF
            LEFT JOIN HRDW_REPLICA.PAYOR_DW.POSTAL_ADDRESS ADDR
            ON ASHF.SUPPLIER_CORR_ADDRESS_KEY=ADDR.POSTAL_ADDRESS_KEY
			AND  ISNULL(UPPER(ADDR.DELETED_FLAG), '') <> 'Y'
            LEFT JOIN HRDW_REPLICA.PAYOR_DW.PROVIDER_TAXONOMY TAX
            ON ASHF.PRIMARY_CLASSIFICATION_KEY=TAX.PROVIDER_TAXONOMY_KEY
			AND  ISNULL(UPPER(TAX.DELETED_FLAG), '') <> 'Y'
            LEFT JOIN HRDW_REPLICA.PAYOR_DW.TAX_ENTITY_HISTORY_FACT TEHF
            ON ASHF.TAX_ENTITY_KEY = TEHF.TAX_ENTITY_KEY
			AND TEHF.VERSION_EFF_DATE_KEY <=  @ln_claim_start_date_key
            AND TEHF.VERSION_EXP_DATE_KEY >   @ln_claim_start_date_key
			AND UPPER(TEHF.TAX_ENTITY_STATUS) NOT IN ('R','V') --Record should not be in Workbasket (Repair,Review)
			AND  ISNULL(UPPER(TEHF.DELETED_FLAG), '') <> 'Y'
            WHERE
                ASHF.SUPPLIER_KEY=@ln_supplier_key
            AND ASHF.VERSION_EFF_DATE_KEY <=  @ln_claim_start_date_key
            AND ASHF.VERSION_EXP_DATE_KEY >   @ln_claim_start_date_key
            AND UPPER(ASHF.SUPPLIER_STATUS) NOT IN ('R','V') --Record should not be in Workbasket (Repair,Review)
			AND ISNULL(UPPER(ASHF.DELETED_FLAG), '') <> 'Y'

       -- DEBUG--  PRINT ' GETTING CLAIM BILLING PROVIDER INFORMATION Completed : ' + CONVERT( varchar, Getdate(),121)

                               /*** GETTING CLAIM'S RENDERING PROVIDER INFORMATION ***/

                               /***IDENTIFYING RENDERING PRACTITIONER KEY BASED ON CLAIM TYPE ***/

                               /*

                               LOGIC: AT CLAIM LEVEL

                                     CLAIM TYPE: INSTITUTIONAL
                                     ->CONSIDER THE  ATTENDING PRACTITIONER AS RENDERING
                                     ->IF ATTENDING PRACTITIONER NOT AVAILBLE CONSIDER SUPPLIER AS RENDERING

                                     CLAIM TYPE: PROFESSIONAL
                                     ->CONSIDER THE  FIRST NON NULL PRACTITIONER AT LINE LEVEL AS RENDERING
                                     ->IF THE PRACTITIONER NOT AVAILBLE CONSIDER SUPPLIER AS RENDERING

                               */


       -- DEBUG--  PRINT ' IDENTIFYING RENDERING PRACTITIONER KEY BASED ON CLAIM TYPE Started : ' + CONVERT( varchar, Getdate(),121)

      SELECT @lv_claim_type_name=UPPER(CLAIM_TYPE_NAME)
      FROM #CLAIM_DETAILS

	--2.3 changes start
	DROP TABLE IF EXISTS  #REFERING_TEMP
	CREATE TABLE  #REFERING_TEMP
    (
      REFERRING_PRACTITIONER_KEY    NUMERIC(19)   NULL,
      PRACTITIONER_HCC_ID           VARCHAR(50)   NULL,
      PRACTITIONER_NPI              VARCHAR(150)  NULL,
      PROVIDER_TAXONOMY_CODE        VARCHAR(50)   NULL,
      PRACTITIONER_LAST_NAME        VARCHAR(107)  NULL,
      PRACTITIONER_FIRST_NAME       VARCHAR(107)  NULL,
      PRACTITIONER_MIDDLE_NAME      VARCHAR(107)  NULL,
      PRACTITIONER_NAME_SUFFIX      VARCHAR(107)  NULL

    )

    IF @ln_referring_practitioner_key IS NOT NULL AND @lv_claim_type_name = 'INSTITUTIONAL'
    BEGIN
       INSERT INTO #REFERING_TEMP  /* FETCHING PRACTITIONER AND PRACTITIONER ADDRESS DETAILS */
       SELECT
          @ln_referring_practitioner_key AS REFERRING_PRACTITIONER_KEY,
          LEFT(PR_HIST.PRACTITIONER_HCC_ID,25),
          LEFT(PR_HIST.PRACTITIONER_NPI,10),
          LEFT(PRV_TX.PROVIDER_TAXONOMY_CODE,10) ,
          LEFT(PR_HIST.PRACTITIONER_LAST_NAME,60),
          LEFT(PR_HIST.PRACTITIONER_FIRST_NAME,35),
          LEFT(PR_HIST.PRACTITIONER_MIDDLE_NAME,25),
          LEFT(PR_HIST.PRACTITIONER_NAME_SUFFIX,10)
       FROM HRDW_REPLICA.PAYOR_DW.ALL_PRACTITIONER_HISTORY_FACT PR_HIST
             LEFT JOIN
            HRDW_REPLICA.PAYOR_DW.PROVIDER_TAXONOMY PRV_TX ON
             PRV_TX.PROVIDER_TAXONOMY_KEY = PR_HIST.PRIMARY_SPECIALTY_KEY
			 AND  ISNULL(UPPER(PRV_TX.DELETED_FLAG), '') <> 'Y'
       WHERE   PR_HIST.PRACTITIONER_KEY = @ln_referring_practitioner_key
         AND  PR_HIST.VERSION_EFF_DATE_KEY <= @ln_claim_start_date_key
         AND  PR_HIST.VERSION_EXP_DATE_KEY >  @ln_claim_start_date_key
         AND  UPPER(PR_HIST.PRACTITIONER_STATUS) NOT IN ('R','V')
		 AND  ISNULL(UPPER(PR_HIST.DELETED_FLAG), '') <> 'Y'
    END
	--2.3 changes end

      IF @lv_claim_type_name='INSTITUTIONAL'
      BEGIN

        SELECT @ln_rendering_key=ATTENDING_PRACTITIONER_KEY FROM #CLAIM_DETAILS

      END

      ELSE IF @lv_claim_type_name='PROFESSIONAL'
      BEGIN

        SELECT
             TOP 1 @ln_rendering_key=ACLF.PRACTITIONER_KEY
        FROM HRDW_REPLICA.PAYOR_DW.ALL_CLAIM_LINE_FACT ACLF
        WHERE
            ACLF.CLAIM_FACT_KEY=@ln_claim_fact_key
        AND ACLF.PRACTITIONER_KEY IS NOT NULL
		AND ISNULL(UPPER(ACLF.DELETED_FLAG), '') <> 'Y'
        ORDER BY ACLF.CLAIM_LINE_FACT_KEY

      END

       -- DEBUG--  PRINT ' IDENTIFYING RENDERING PRACTITIONER KEY BASED ON CLAIM TYPE Completed : ' + CONVERT( varchar, Getdate(),121)

       -- DEBUG--  PRINT ' GETTING CLAIM RENDERING PROVIDER INFORMATION Started : ' + CONVERT( varchar, Getdate(),121)
      IF @ln_rendering_key IS NOT NULL
      BEGIN
            INSERT INTO #RENDERING_PROV_DETAILS
            SELECT
                 @ln_claim_fact_key
                ,LEFT(APHF.PRACTITIONER_HCC_ID,25)
                ,LEFT(APHF.PRACTITIONER_NPI,10)
                ,LEFT(TAX.PROVIDER_TAXONOMY_CODE,10)
                ,REPLACE(APHF.TAX_ID,'-','') AS TAX_ID
                ,'P'
                ,LEFT(APHF.PRACTITIONER_LAST_NAME,60)
                ,LEFT(APHF.PRACTITIONER_FIRST_NAME,35)
                ,LEFT(APHF.PRACTITIONER_MIDDLE_NAME,25)
                ,LEFT(APHF.PRACTITIONER_NAME_SUFFIX,10)
                ,LEFT(ADDR.ADDRESS_LINE,55)
                ,LEFT(ADDR.ADDRESS_LINE_2,55)
                ,LEFT(ADDR.CITY_NAME,30)
                ,LEFT(ADDR.STATE_CODE,2)
                ,ADDR.COUNTRY_CODE
                ,LEFT(ADDR.ZIP_CODE,5) AS ZIP_CODE
                ,ADDR.ZIP_4_CODE
            FROM HRDW_REPLICA.PAYOR_DW.ALL_PRACTITIONER_HISTORY_FACT APHF
            LEFT JOIN HRDW_REPLICA.PAYOR_DW.POSTAL_ADDRESS ADDR
            ON APHF.PRACTITIONER_CORR_ADDR_KEY=ADDR.POSTAL_ADDRESS_KEY
			AND ISNULL(UPPER(ADDR.DELETED_FLAG), '') <> 'Y'
            LEFT JOIN HRDW_REPLICA.PAYOR_DW.PROVIDER_TAXONOMY TAX
            ON APHF.PRIMARY_SPECIALTY_KEY=TAX.PROVIDER_TAXONOMY_KEY
			AND ISNULL(UPPER(TAX.DELETED_FLAG), '') <> 'Y'
            WHERE
                APHF.PRACTITIONER_KEY=@ln_rendering_key
            AND APHF.VERSION_EFF_DATE_KEY <=  @ln_claim_start_date_key
            AND APHF.VERSION_EXP_DATE_KEY >   @ln_claim_start_date_key
            AND UPPER(APHF.PRACTITIONER_STATUS) NOT IN ('R','V') --Record should not be in Workbasket (Repair,Review)
			AND ISNULL(UPPER(APHF.DELETED_FLAG), '') <> 'Y'
      END

      ELSE IF @ln_supplier_key IS NOT NULL
      BEGIN
            INSERT INTO #RENDERING_PROV_DETAILS
            SELECT *
            FROM #BILLING_PROV_DETAILS
      END

       -- DEBUG--  PRINT ' GETTING CLAIM RENDERING PROVIDER INFORMATION Completed : ' + CONVERT( varchar, Getdate(),121)

                               /*** GETTING CLAIM'S AMOUNT INFORMATION ***/

       -- DEBUG--  PRINT ' GETTING CLAIM AMOUNT INFORMATION Started : ' + CONVERT( varchar, Getdate(),121)

      INSERT INTO #CLAIM_AMOUNTS
      SELECT
            ACLF.CLAIM_FACT_KEY
           ,SUM(ACLF.BILLED_AMOUNT)
           ,SUM(ACLF.BASE_NON_COVERED_AMOUNT)
           ,SUM(ACLF.BASE_ALLOWED_AMOUNT)
           ,SUM(ACLF.BASE_PAID_AMOUNT)
		   ,SUM(BASE_COINSURANCE_AMOUNT)--2.3 changes
		   ,SUM(BASE_COPAY_AMOUNT)--2.3 changes
		   ,SUM(BASE_DEDUCTIBLE_AMOUNT)--2.3 changes
		   ,SUM(ISNULL(BASE_COINSURANCE_AMOUNT,0) + ISNULL(BASE_COPAY_AMOUNT,0) + ISNULL(BASE_DEDUCTIBLE_AMOUNT,0))--2.3 changes
      FROM #CLAIM_DETAILS CLAIM
			INNER JOIN HRDW_REPLICA.PAYOR_DW.ALL_CLAIM_LINE_FACT ACLF
			ON CLAIM.CLAIM_FACT_KEY=ACLF.CLAIM_FACT_KEY
	  WHERE
		  ISNULL(UPPER(ACLF.DELETED_FLAG), '') <> 'Y'
		  AND NOT EXISTS (SELECT 1 FROM HRDW_REPLICA.PAYOR_DW.ALL_CLAIM_LINE_FACT ACLF1
						  WHERE
						  	ACLF.CLAIM_FACT_KEY= ACLF1.CLAIM_FACT_KEY
						  AND ACLF.CLAIM_LINE_FACT_KEY <> ACLF1.CLAIM_LINE_FACT_KEY
						  AND ACLF.CLAIM_LINE_HCC_ID = ACLF1.ORIGINAL_LINE_NUMBER
						  AND UPPER(ACLF1.IS_SPLIT) ='Y'
						  AND ISNULL(UPPER(ACLF1.DELETED_FLAG), '') <> 'Y')
      GROUP BY ACLF.CLAIM_FACT_KEY

       -- DEBUG--  PRINT ' GETTING CLAIM AMOUNT INFORMATION Completed : ' + CONVERT( varchar, Getdate(),121)

                               /*** INSERT CLAIM DETAILS INTO THE TEMP TABLE ***/
        -- DEBUG--  PRINT ' INSERT CLAIM DETAILS INTO THE TEMP TABLE Started : ' + CONVERT( varchar, Getdate(),121)

      IF UPPER(@lv_claim_type_name)='INSTITUTIONAL'
      BEGIN

          CREATE TABLE #INSTITUTIONAL_CLAIM_DETAILS (
						   CLAIM_TYPE_NAME                          VARCHAR(20)
					      ,CLAIM_ID                              	VARCHAR(25)
					      ,CLAIM_FACT_KEY                        	NUMERIC(19)
						  ,CLAIM_TYPE                            	VARCHAR(20)
						  ,PAYER_SHORT								VARCHAR(5)
					      ,SUB_ID                                	VARCHAR(25)
					      ,DEP_ID                                	VARCHAR(10)   DEFAULT ''
					      ,PATIENT_DOB                           	DATE
					      ,PATIENT_GENDER_ID                     	VARCHAR(10)
					      ,PATIENT_SSN                           	VARCHAR(9)
					      ,PATIENT_ID                            	VARCHAR(20)
					      ,PATIENT_CONTROL_NUMBER                	VARCHAR(24)
					      ,PATIENT_LAST_NAME                     	VARCHAR(60)
					      ,PATIENT_FIRST_NAME                    	VARCHAR(35)
					      ,PATIENT_MIDDLE_NAME                   	VARCHAR(25)
					      ,PATIENT_NAME_SUFFIX                   	VARCHAR(10)
					      ,RELATIONSHIP_TO_SUB                   	VARCHAR(2)
					      ,PROVIDER_CLAIM_ID                     	VARCHAR(25)  DEFAULT ''
					      ,MED_REC_NO                            	VARCHAR(24)  DEFAULT ''
					      ,BILL_TYPE                             	VARCHAR(4)
					      ,CONDITION_CODE_1                      	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_2                      	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_3                      	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_4                      	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_5                      	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_6                      	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_7                      	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_8                      	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_9                      	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_10                     	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_11                     	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_12                     	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_13                     	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_14                     	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_15                     	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_16                     	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_17                     	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_18                     	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_19                     	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_20                     	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_21                     	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_22                     	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_23                     	VARCHAR(2)   DEFAULT ''
					      ,CONDITION_CODE_24                     	VARCHAR(2)   DEFAULT ''
					      ,VALUE_CODE_1                          	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_1                        	NUMERIC(19,2)
					      ,VALUE_CODE_2                          	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_2                        	NUMERIC(19,2)
					      ,VALUE_CODE_3                          	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_3                        	NUMERIC(19,2)
					      ,VALUE_CODE_4                          	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_4                        	NUMERIC(19,2)
					      ,VALUE_CODE_5                          	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_5                        	NUMERIC(19,2)
					      ,VALUE_CODE_6                          	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_6                        	NUMERIC(19,2)
					      ,VALUE_CODE_7                          	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_7                        	NUMERIC(19,2)
					      ,VALUE_CODE_8                          	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_8                        	NUMERIC(19,2)
					      ,VALUE_CODE_9                          	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_9                        	NUMERIC(19,2)
					      ,VALUE_CODE_10                         	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_10                       	NUMERIC(19,2)
					      ,VALUE_CODE_11                         	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_11                       	NUMERIC(19,2)
					      ,VALUE_CODE_12                         	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_12                       	NUMERIC(19,2)
					      ,VALUE_CODE_13                         	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_13                       	NUMERIC(19,2)
					      ,VALUE_CODE_14                         	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_14                       	NUMERIC(19,2)
					      ,VALUE_CODE_15                         	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_15                       	NUMERIC(19,2)
					      ,VALUE_CODE_16                         	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_16                       	NUMERIC(19,2)
					      ,VALUE_CODE_17                         	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_17                       	NUMERIC(19,2)
					      ,VALUE_CODE_18                         	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_18                       	NUMERIC(19,2)
					      ,VALUE_CODE_19                         	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_19                       	NUMERIC(19,2)
					      ,VALUE_CODE_20                         	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_20                       	NUMERIC(19,2)
					      ,VALUE_CODE_21                         	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_21                       	NUMERIC(19,2)
					      ,VALUE_CODE_22                         	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_22                       	NUMERIC(19,2)
					      ,VALUE_CODE_23                         	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_23                       	NUMERIC(19,2)
					      ,VALUE_CODE_24                         	VARCHAR(2)   DEFAULT ''
					      ,VALUE_AMOUNT_24                       	NUMERIC(19,2)
					      ,CLAIM_DOS_FROM                        	DATE
					      ,CLAIM_DOS_TO                          	DATE
					      ,ADMIT_DATE                            	DATE
					      ,ADMIT_TIME_HH                         	NUMERIC(2)
					      ,ADMIT_TIME_MM                         	NUMERIC(2)
					      ,ADMIT_TYPE                            	VARCHAR(1)   DEFAULT ''
					      ,ADMIT_SOURCE_CODE                     	VARCHAR(1)   DEFAULT ''
					      ,DISCHARGE_DATE                        	DATE
					      ,DISCHARGE_TIME_HH                     	NUMERIC(2)
					      ,DISCHARGE_TIME_MM                     	NUMERIC(2)
					      ,DISCHARGE_STATUS                      	VARCHAR(2)
					      ,SUB_DRG                               	VARCHAR(5)
					      ,SUB_SEVERITY                          	VARCHAR(2)   DEFAULT NULL
					      ,ALLOWED_DRG                           	VARCHAR(5)
					      ,ALLOWED_SEVERITY                      	VARCHAR(2)   DEFAULT NULL
					      ,DRG_INLIER_AMOUNT                     	NUMERIC(19,2)
					      ,DRG_OUTLIER_AMOUNT                    	NUMERIC(19,2)
					      ,GROUPER_ID                            	VARCHAR(30)  DEFAULT ''
					      ,REGULATORY_STATE                      	VARCHAR(3)   DEFAULT ''
					      ,PRINCIPAL_DIAGNOSIS_QUAL              	VARCHAR(1)
					      ,PRINCIPAL_DIAGNOSIS                   	VARCHAR(7)
					      ,PRINCIPAL_DIAGNOSIS_POA               	VARCHAR(1)
						  ,OTHER_DIAGNOSIS_LIST                   	VARCHAR(MAX)
					      ,ADMITTING_DIAGNOSIS_QUAL              	VARCHAR(1)
					      ,ADMITTING_DIAGNOSIS                   	VARCHAR(7)
						  ,EXTERNAL_CAUSE_OF_INJURY_LIST          	VARCHAR(MAX)
						  ,PATIENTS_REASON_FOR_VISIT_LIST         	VARCHAR(MAX)
					      ,PRINCIPAL_PROCEDURE_QUAL              	VARCHAR(1)
					      ,PRINCIPAL_PROCEDURE                   	VARCHAR(7)
					      ,PRINCIPAL_PROCEDURE_DATE              	DATE
						  ,OTHER_PROCEDURE_LIST                   	VARCHAR(MAX)
					      ,AUTH_NO_A                             	VARCHAR(30)  DEFAULT ''
					      ,AUTH_NO_B                             	VARCHAR(30)  DEFAULT ''
					      ,AUTH_NO_C                             	VARCHAR(30)  DEFAULT ''
					      ,RENDERING_PROVIDER_ID                 	VARCHAR(25)
					      ,RENDERING_TAXGROUP_ID                 	VARCHAR(9)
					      ,RENDERING_NPI                         	NUMERIC(10)
					      ,RENDERING_STATE_LICENSE               	VARCHAR(10)  DEFAULT ''
					      ,RENDERING_SUBSPEC_ID                  	VARCHAR(10)
					      ,RENDERING_ENTITY_TYPE                 	VARCHAR(1)
					      ,RENDERING_LAST_NAME                   	VARCHAR(60)
					      ,RENDERING_FIRST_NAME                  	VARCHAR(35)
					      ,RENDERING_MIDDLE_NAME                 	VARCHAR(25)
					      ,RENDERING_NAME_SUFFIX                 	VARCHAR(10)
					      ,RENDERING_STREET_ADDRESS_1            	VARCHAR(55)
					      ,RENDERING_STREET_ADDRESS_2            	VARCHAR(55)
					      ,RENDERING_CITY                        	VARCHAR(30)
					      ,RENDERING_STATE                       	VARCHAR(2)
					      ,RENDERING_ZIP                         	VARCHAR(5)
					      ,RENDERING_ZIP_PLUS_4                  	VARCHAR(4)
					      ,RENDERING_COUNTRY                     	VARCHAR(3)
					      ,RENDERING_COUNTRY_SUBDIVISION         	VARCHAR(3)    DEFAULT ''
					      ,RENDERING_PHONE_COUNTRY_CODE          	VARCHAR(5)    DEFAULT ''
					      ,RENDERING_PHONE                       	VARCHAR(10)   DEFAULT ''
					      ,RENDERING_PHONE_EXT                   	VARCHAR(6)    DEFAULT ''
					      ,RENDERING_ALT_PHONE_COUNTRY_CODE      	VARCHAR(5)    DEFAULT ''
					      ,RENDERING_ALT_PHONE                   	VARCHAR(10)   DEFAULT ''
					      ,RENDERING_ALT_PHONE_EXT               	VARCHAR(6)    DEFAULT ''
					      ,RENDERING_FAX_COUNTRY_CODE            	VARCHAR(5)    DEFAULT ''
					      ,RENDERING_FAX                         	VARCHAR(10)   DEFAULT ''
					      ,BILLING_PROVIDER_ID                   	VARCHAR(25)
					      ,BILLING_TAXGROUP_ID                   	VARCHAR(9)
					      ,BILLING_STATE_LICENSE                 	VARCHAR(10)   DEFAULT ''
					      ,BILLING_SUBSPEC_ID                    	VARCHAR(10)   DEFAULT ''
					      ,BILLING_CURRENCY_CODE                 	VARCHAR(3)    DEFAULT ''
					      ,BILLING_ENTITY_TYPE                   	VARCHAR(1)    DEFAULT ''
					      ,BILLING_NAME                          	VARCHAR(255)  DEFAULT ''
					      ,BILLING_ADDRESS_1                     	VARCHAR(255)  DEFAULT ''
					      ,BILLING_ADDRESS_2                     	VARCHAR(255)  DEFAULT ''
					      ,BILLING_CITY                          	VARCHAR(50)   DEFAULT ''
					      ,BILLING_STATE                         	VARCHAR(2)    DEFAULT ''
						  ,BILLING_ZIP               				VARCHAR(5)    DEFAULT ''--2.3 changes
						  ,BILLING_ZIP_PLUS_4        				VARCHAR(4)    DEFAULT ''--2.3 changes
					      ,BILLING_COUNTRY                       	VARCHAR(3)    DEFAULT ''
					      ,BILLING_COUNTRY_SUBDIVISION           	VARCHAR(3)    DEFAULT ''
					      ,BILLING_CONTACT                       	VARCHAR(60)   DEFAULT ''
					      ,BILLING_PHONE_COUNTRY_CODE            	VARCHAR(5)    DEFAULT ''
					      ,BILLING_PHONE                         	VARCHAR(10)   DEFAULT ''
					      ,BILLING_PHONE_EXT                     	VARCHAR(6)    DEFAULT ''
					      ,BILLING_ALT_PHONE_COUNTRY_CODE        	VARCHAR(5)    DEFAULT ''
					      ,BILLING_ALT_PHONE                     	VARCHAR(10)   DEFAULT ''
					      ,BILLING_ALT_PHONE_EXT                 	VARCHAR(6)    DEFAULT ''
					      ,BILLING_FAX_COUNTRY_CODE              	VARCHAR(5)    DEFAULT ''
					      ,BILLING_FAX                           	VARCHAR(10)   DEFAULT ''
					      ,CLAIM_BILLED_AMOUNT                   	NUMERIC(19,2)
					      ,CLAIM_NONCOVERED_AMOUNT               	NUMERIC(19,2)
					      ,CLAIM_ALLOWED_AMOUNT                  	NUMERIC(19,2)
					      ,CLAIM_PAID_AMOUNT                     	NUMERIC(19,2)
					      ,CLAIM_PAID_DATE                       	DATE
					      ,DRG_PAYABLE_YN                        	VARCHAR(1)     DEFAULT ''
					      ,ASSIGNMENT_OF_BENEFITS                	VARCHAR(1)     DEFAULT ''
					      ,PRIOR_PAYMENTS                        	NUMERIC(19,2)
					      ,AUDIT_BYPASS_CODE                     	NUMERIC(20)
					      ,DATE_RECEIVED_CLIENT                  	DATE
						  ,DATE_ADJUDICATED							DATE
					      ,CLAIM_APPROVED_BY                     	VARCHAR	(15)   DEFAULT ''
					      ,LETTER_PLAN_CODE                      	VARCHAR	(6)    DEFAULT ''
					      ,CLAIM_ID_ORIG                         	VARCHAR	(25)
					      ,GROUP_ID                              	VARCHAR	(15)   DEFAULT ''
					      ,GROUP_NAME                            	VARCHAR	(60)   DEFAULT ''
					      ,RISK_POOL                             	VARCHAR	(60)   DEFAULT ''
					      ,CLAIM_DENIAL_RSN_CD                   	VARCHAR	(10)   DEFAULT ''
					      ,MORTALITY_RISK                        	VARCHAR	(2)    DEFAULT ''
					      ,BIRTH_WEIGHT                          	NUMERIC	(6)
					      ,ALC_DAYS                              	NUMERIC	(4)
					      ,ALC_AMOUNT                            	NUMERIC (19,2)
					      ,PAYER_NAME                            	VARCHAR	(60)   DEFAULT ''
					      ,PAYER_ADDRESS_1                       	VARCHAR	(55)   DEFAULT ''
					      ,PAYER_ADDRESS_2                       	VARCHAR	(55)   DEFAULT ''
					      ,PAYER_CITY                            	VARCHAR	(30)   DEFAULT ''
					      ,PAYER_STATE                           	VARCHAR	(2)    DEFAULT ''
					      ,PAYER_ZIP                             	VARCHAR	(10)    DEFAULT ''
					      ,PAYER_ZIP_PLUS_4                      	VARCHAR	(4)    DEFAULT ''
					      ,PAYER_COUNTRY                         	VARCHAR	(3)    DEFAULT ''
					      ,PAYER_COUNTRY_SUBDIVISION             	VARCHAR	(3)    DEFAULT ''
					      ,CLAIM_FILING_INDICATOR                	VARCHAR	(2)    DEFAULT ''
					      ,SUB_LAST_NAME                         	VARCHAR	(60)   DEFAULT ''
					      ,SUB_FIRST_NAME                        	VARCHAR	(35)   DEFAULT ''
					      ,SUB_MIDDLE_NAME                       	VARCHAR	(25)   DEFAULT ''
					      ,SUB_NAME_SUFFIX                       	VARCHAR	(10)   DEFAULT ''
					      ,SUB_ADDRESS_1                         	VARCHAR	(55)   DEFAULT ''
					      ,SUB_ADDDRESS_2                        	VARCHAR	(55)   DEFAULT ''
					      ,SUB_CITY                              	VARCHAR	(30)   DEFAULT ''
					      ,SUB_STATE                             	VARCHAR	(2)    DEFAULT ''
					      ,SUB_ZIP                               	VARCHAR	(5)   DEFAULT ''
					      ,SUB_ZIP_PLUS_4                        	VARCHAR	(4)    DEFAULT ''
					      ,SUB_COUNTRY                           	VARCHAR	(3)    DEFAULT ''
					      ,SUB_COUNTRY_SUBDIVISION               	VARCHAR	(3)    DEFAULT ''
					      ,SUB_DOB                               	DATE
					      ,SUB_GENDER_ID                         	VARCHAR	(10)   DEFAULT ''
					      ,SUB_SSN                               	VARCHAR	(9)    DEFAULT ''
					      ,PATIENT_ADDRESS_1                     	VARCHAR	(55)   DEFAULT ''
					      ,PATIENT_ADDRESS_2                     	VARCHAR	(55)   DEFAULT ''
					      ,PATIENT_CITY                          	VARCHAR	(30)   DEFAULT ''
					      ,PATIENT_STATE                         	VARCHAR	(2)    DEFAULT ''
					      ,PATIENT_ZIP                           	VARCHAR	(10)    DEFAULT ''
					      ,PATIENT_ZIP_PLUS_4                    	VARCHAR	(4)    DEFAULT ''
					      ,PATIENT_COUNTRY                       	VARCHAR	(3)    DEFAULT ''
					      ,PATIENT_COUNTRY_SUBDIVISION           	VARCHAR	(3)    DEFAULT ''
					      ,PAY_TO_ENTITY_TYPE                    	VARCHAR	(1)    DEFAULT ''
					      ,PAY_TO_NAME                           	VARCHAR	(60)   DEFAULT ''
					      ,PAY_TO_ADDRESS_1                      	VARCHAR	(55)   DEFAULT ''
					      ,PAY_TO_ADDRESS_2                      	VARCHAR	(55)   DEFAULT ''
					      ,PAY_TO_CITY                           	VARCHAR	(30)   DEFAULT ''
					      ,PAY_TO_STATE                          	VARCHAR	(2)    DEFAULT ''
					      ,PAY_TO_ZIP                            	VARCHAR	(5)   DEFAULT ''
					      ,PAY_TO_ZIP_PLUS_4                     	VARCHAR	(4)    DEFAULT ''
					      ,PAY_TO_COUNTRY                        	VARCHAR	(3)    DEFAULT ''
					      ,PAY_TO_COUNTRY_SUBDIVISION            	VARCHAR	(3)    DEFAULT ''
					      ,ATTENDING_PROVIDER_ID                 	VARCHAR	(25)   DEFAULT ''
					      ,ATTENDING_TAXGROUP_ID                 	VARCHAR	(9)   DEFAULT ''
					      ,ATTENDING_NPI                         	NUMERIC	(10)
					      ,ATTENDING_STATE_LICENSE               	VARCHAR	(10)   DEFAULT ''
					      ,ATTENDING_SUBSPEC_ID                  	VARCHAR	(10)   DEFAULT ''
					      ,ATTENDING_ENTITY_TYPE                 	VARCHAR	(1)    DEFAULT ''
					      ,ATTENDING_LAST_NAME                   	VARCHAR	(60)   DEFAULT ''
					      ,ATTENDING_FIRST_NAME                  	VARCHAR	(35)   DEFAULT ''
					      ,ATTENDING_MIDDLE_NAME                 	VARCHAR	(25)   DEFAULT ''
					      ,ATTENDING_NAME_SUFFIX                 	VARCHAR	(10)   DEFAULT ''
					      ,OPERATING_PROVIDER_ID                 	VARCHAR	(25)   DEFAULT ''
					      ,OPERATING_TAXGROUP_ID                 	VARCHAR	(9)    DEFAULT ''
					      ,OPERATING_NPI                         	NUMERIC	(10)
					      ,OPERATING_STATE_LICENSE               	VARCHAR	(10)   DEFAULT ''
					      ,OPERATING_SUBSPEC_ID                  	VARCHAR	(10)   DEFAULT ''
					      ,OPERATING_ENTITY_TYPE                 	VARCHAR	(1)    DEFAULT ''
					      ,OPERATING_LAST_NAME                   	VARCHAR	(60)   DEFAULT ''
					      ,OPERATING_FIRST_NAME                  	VARCHAR	(35)   DEFAULT ''
					      ,OPERATING_MIDDLE_NAME                 	VARCHAR	(25)   DEFAULT ''
					      ,OPERATING_NAME_SUFFIX                 	VARCHAR	(10)   DEFAULT ''
					      ,OTHER_OPERATING_PROVIDER_ID           	VARCHAR	(25)   DEFAULT ''
					      ,OTHER_OPERATING_TAXGROUP_ID           	VARCHAR	(9)    DEFAULT ''
					      ,OTHER_OPERATING_NPI                   	NUMERIC	(10)
					      ,OTHER_OPERATING_STATE_LICENSE         	VARCHAR	(10)   DEFAULT ''
					      ,OTHER_OPERATING_SUBSPEC_ID            	VARCHAR	(10)   DEFAULT ''
					      ,OTHER_OPERATING_ENTITY_TYPE           	VARCHAR	(1)    DEFAULT ''
					      ,OTHER_OPERATING_LAST_NAME             	VARCHAR	(60)   DEFAULT ''
					      ,OTHER_OPERATING_FIRST_NAME            	VARCHAR	(35)   DEFAULT ''
					      ,OTHER_OPERATING_MIDDLE_NAME           	VARCHAR	(25)   DEFAULT ''
					      ,OTHER_OPERATING_NAME_SUFFIX           	VARCHAR	(10)   DEFAULT ''
					      ,REFERRING_PCP_YN                      	VARCHAR	(1)    DEFAULT ''
					      ,REFERRING_PROVIDER_ID                 	VARCHAR	(25)   DEFAULT ''
					      ,REFERRING_TAXGROUP_ID                 	VARCHAR	(9)    DEFAULT ''
					      ,REFERRING_NPI                         	NUMERIC	(10)
					      ,REFERRING_STATE_LICENSE               	VARCHAR	(10)   DEFAULT ''
					      ,REFERRING_SUBSPEC_ID                  	VARCHAR	(10)   DEFAULT ''
					      ,REFERRING_ENTITY_TYPE                 	VARCHAR	(1)    DEFAULT ''
					      ,REFERRING_LAST_NAME                   	VARCHAR	(60)   DEFAULT ''
					      ,REFERRING_FIRST_NAME                  	VARCHAR	(35)   DEFAULT ''
					      ,REFERRING_MIDDLE_NAME                 	VARCHAR	(25)   DEFAULT ''
					      ,REFERRING_NAME_SUFFIX                 	VARCHAR	(10)   DEFAULT ''
					      ,PATIENT_STATUS_CODE                   	VARCHAR	(2)    DEFAULT ''
					      ,CLAIM_ATTACHMENT_TYPE_CODE            	VARCHAR	(2)    DEFAULT ''
					      ,CLAIM_ATTACHMENT_TRANSMIT_CODE        	VARCHAR	(2)    DEFAULT ''
					      ,CLAIM_ACN                             	VARCHAR	(80)   DEFAULT ''
					      ,PATIENT_RESP_AMOUNT                   	NUMERIC (19,2)
					      ,AUTH_EXCEPTION_CODE                   	VARCHAR	(4)    DEFAULT ''
					      ,REFERRAL_ID                           	VARCHAR	(30)   DEFAULT ''
					      ,PRO_AUTH_ID                           	VARCHAR	(24)   DEFAULT ''
					      ,CLAIM_NOTE_REF_CODE                   	VARCHAR	(3)    DEFAULT ''
					      ,CLAIM_NOTE_TEXT                       	VARCHAR	(80)   DEFAULT ''
					      ,EPSDT_REFERRAL_YN                     	VARCHAR	(1)    DEFAULT ''
					      ,EPSDT_COND_INDICATOR_1                	VARCHAR	(3)    DEFAULT ''
					      ,EPSDT_COND_INDICATOR_2                	VARCHAR	(3)    DEFAULT ''
					      ,EPSDT_COND_INDICATOR_3                	VARCHAR	(3)    DEFAULT ''
					      ,OCCUR_SPAN_CODE_1                     	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_1                     	DATE
					      ,OCCUR_SPAN_TO_1                       	DATE
					      ,OCCUR_SPAN_CODE_2                     	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_2                     	DATE
					      ,OCCUR_SPAN_TO_2                       	DATE
					      ,OCCUR_SPAN_CODE_3                     	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_3                     	DATE
					      ,OCCUR_SPAN_TO_3                       	DATE
					      ,OCCUR_SPAN_CODE_4                     	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_4                     	DATE
					      ,OCCUR_SPAN_TO_4                       	DATE
					      ,OCCUR_SPAN_CODE_5                     	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_5                     	DATE
					      ,OCCUR_SPAN_TO_5                       	DATE
					      ,OCCUR_SPAN_CODE_6                     	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_6                     	DATE
					      ,OCCUR_SPAN_TO_6                       	DATE
					      ,OCCUR_SPAN_CODE_7                     	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_7                     	DATE
					      ,OCCUR_SPAN_TO_7                       	DATE
					      ,OCCUR_SPAN_CODE_8                     	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_8                     	DATE
					      ,OCCUR_SPAN_TO_8                       	DATE
					      ,OCCUR_SPAN_CODE_9                     	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_9                     	DATE
					      ,OCCUR_SPAN_TO_9                       	DATE
					      ,OCCUR_SPAN_CODE_10                    	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_10                    	DATE
					      ,OCCUR_SPAN_TO_10                      	DATE
					      ,OCCUR_SPAN_CODE_11                    	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_11                    	DATE
					      ,OCCUR_SPAN_TO_11                      	DATE
					      ,OCCUR_SPAN_CODE_12                    	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_12                    	DATE
					      ,OCCUR_SPAN_TO_12                      	DATE
					      ,OCCUR_SPAN_CODE_13                    	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_13                    	DATE
					      ,OCCUR_SPAN_TO_13                      	DATE
					      ,OCCUR_SPAN_CODE_14                    	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_14                    	DATE
					      ,OCCUR_SPAN_TO_14                      	DATE
					      ,OCCUR_SPAN_CODE_15                    	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_15                    	DATE
					      ,OCCUR_SPAN_TO_15                      	DATE
					      ,OCCUR_SPAN_CODE_16                    	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_16                    	DATE
					      ,OCCUR_SPAN_TO_16                      	DATE
					      ,OCCUR_SPAN_CODE_17                    	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_17                    	DATE
					      ,OCCUR_SPAN_TO_17                      	DATE
					      ,OCCUR_SPAN_CODE_18                    	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_18                    	DATE
					      ,OCCUR_SPAN_TO_18                      	DATE
					      ,OCCUR_SPAN_CODE_19                    	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_19                    	DATE
					      ,OCCUR_SPAN_TO_19                      	DATE
					      ,OCCUR_SPAN_CODE_20                    	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_20                    	DATE
					      ,OCCUR_SPAN_TO_20                      	DATE
					      ,OCCUR_SPAN_CODE_21                    	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_21                    	DATE
					      ,OCCUR_SPAN_TO_21                      	DATE
					      ,OCCUR_SPAN_CODE_22                    	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_22                    	DATE
					      ,OCCUR_SPAN_TO_22                      	DATE
					      ,OCCUR_SPAN_CODE_23                    	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_23                    	DATE
					      ,OCCUR_SPAN_TO_23                      	DATE
					      ,OCCUR_SPAN_CODE_24                    	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_SPAN_FROM_24                    	DATE
					      ,OCCUR_SPAN_TO_24                      	DATE
					      ,OCCUR_CODE_1                          	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_1                          	DATE
					      ,OCCUR_CODE_2                          	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_2                          	DATE
					      ,OCCUR_CODE_3                          	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_3                          	DATE
					      ,OCCUR_CODE_4                          	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_4                          	DATE
					      ,OCCUR_CODE_5                          	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_5                          	DATE
					      ,OCCUR_CODE_6                          	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_6                          	DATE
					      ,OCCUR_CODE_7                          	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_7                          	DATE
					      ,OCCUR_CODE_8                          	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_8                          	DATE
					      ,OCCUR_CODE_9                          	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_9                          	DATE
					      ,OCCUR_CODE_10                         	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_10                         	DATE
					      ,OCCUR_CODE_11                         	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_11                         	DATE
					      ,OCCUR_CODE_12                         	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_12                         	DATE
					      ,OCCUR_CODE_13                         	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_13                         	DATE
					      ,OCCUR_CODE_14                         	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_14                         	DATE
					      ,OCCUR_CODE_15                         	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_15                         	DATE
					      ,OCCUR_CODE_16                         	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_16                         	DATE
					      ,OCCUR_CODE_17                         	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_17                         	DATE
					      ,OCCUR_CODE_18                         	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_18                         	DATE
					      ,OCCUR_CODE_19                         	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_19                         	DATE
					      ,OCCUR_CODE_20                         	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_20                         	DATE
					      ,OCCUR_CODE_21                         	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_21                         	DATE
					      ,OCCUR_CODE_22                         	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_22                         	DATE
					      ,OCCUR_CODE_23                         	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_23                         	DATE
					      ,OCCUR_CODE_24                         	VARCHAR	(2)    DEFAULT ''
					      ,OCCUR_DATE_24                         	DATE
					      ,ATTACHMENT_TYPE_CODE_1                	VARCHAR	(2)    DEFAULT ''
					      ,ATTACHMENT_TRANSMIT_CODE_1            	VARCHAR	(2)    DEFAULT ''
					      ,ACN_1                                 	VARCHAR	(24)   DEFAULT ''
					      ,ATTACHMENT_TYPE_CODE_2                	VARCHAR	(2)    DEFAULT ''
					      ,ATTACHMENT_TRANSMIT_CODE_2            	VARCHAR	(2)    DEFAULT ''
					      ,ACN_2                                 	VARCHAR	(24)   DEFAULT ''
					      ,ATTACHMENT_TYPE_CODE_3                	VARCHAR	(2)    DEFAULT ''
					      ,ATTACHMENT_TRANSMIT_CODE_3            	VARCHAR	(2)    DEFAULT ''
					      ,ACN_3                                 	VARCHAR	(24)   DEFAULT ''
					      ,ATTACHMENT_TYPE_CODE_4                	VARCHAR	(2)    DEFAULT ''
					      ,ATTACHMENT_TRANSMIT_CODE_4            	VARCHAR	(2)    DEFAULT ''
					      ,ACN_4                                 	VARCHAR	(24)   DEFAULT ''
					      ,ATTACHMENT_TYPE_CODE_5                	VARCHAR	(2)    DEFAULT ''
					      ,ATTACHMENT_TRANSMIT_CODE_5            	VARCHAR	(2)    DEFAULT ''
					      ,ACN_5                                 	VARCHAR	(24)   DEFAULT ''
					      ,ATTACHMENT_TYPE_CODE_6                	VARCHAR	(2)    DEFAULT ''
					      ,ATTACHMENT_TRANSMIT_CODE_6            	VARCHAR	(2)    DEFAULT ''
					      ,ACN_6                                 	VARCHAR	(24)   DEFAULT ''
					      ,ATTACHMENT_TYPE_CODE_7                	VARCHAR	(2)    DEFAULT ''
					      ,ATTACHMENT_TRANSMIT_CODE_7            	VARCHAR	(2)    DEFAULT ''
					      ,ACN_7                                 	VARCHAR	(24)   DEFAULT ''
					      ,ATTACHMENT_TYPE_CODE_8                	VARCHAR	(2)    DEFAULT ''
					      ,ATTACHMENT_TRANSMIT_CODE_8            	VARCHAR	(2)    DEFAULT ''
					      ,ACN_8                                 	VARCHAR	(24)   DEFAULT ''
					      ,ATTACHMENT_TYPE_CODE_9                	VARCHAR	(2)    DEFAULT ''
					      ,ATTACHMENT_TRANSMIT_CODE_9            	VARCHAR	(2)    DEFAULT ''
					      ,ACN_9                                 	VARCHAR	(24)   DEFAULT ''
					      ,ATTACHMENT_TYPE_CODE_10               	VARCHAR	(2)    DEFAULT ''
					      ,ATTACHMENT_TRANSMIT_CODE_10           	VARCHAR	(2)    DEFAULT ''
					      ,ACN_10                                	VARCHAR	(24)   DEFAULT ''
					      ,CLAIM_ADJ_CODE_1                      	VARCHAR	(10)   DEFAULT ''
					      ,CLAIM_ADJ_TYPE_1                      	VARCHAR	(2)    DEFAULT ''
					      ,CLAIM_ADJUSTOR_ID_1                   	VARCHAR	(20)   DEFAULT ''
					      ,CLAIM_ADJ_CODE_2                      	VARCHAR	(10)   DEFAULT ''
					      ,CLAIM_ADJ_TYPE_2                      	VARCHAR	(2)    DEFAULT ''
					      ,CLAIM_ADJUSTOR_ID_2                   	VARCHAR	(20)   DEFAULT ''
					      ,CLAIM_ADJ_CODE_3                      	VARCHAR	(10)   DEFAULT ''
					      ,CLAIM_ADJ_TYPE_3                      	VARCHAR	(2)    DEFAULT ''
					      ,CLAIM_ADJUSTOR_ID_3                   	VARCHAR	(20)   DEFAULT ''
					      ,CLAIM_ADJ_CODE_4                      	VARCHAR	(10)   DEFAULT ''
					      ,CLAIM_ADJ_TYPE_4                      	VARCHAR	(2)    DEFAULT ''
					      ,CLAIM_ADJUSTOR_ID_4                   	VARCHAR	(20)   DEFAULT ''
					      ,CLAIM_ADJ_CODE_5                      	VARCHAR	(10)   DEFAULT ''
					      ,CLAIM_ADJ_TYPE_5                      	VARCHAR	(2)    DEFAULT ''
					      ,CLAIM_ADJUSTOR_ID_5                   	VARCHAR	(20)   DEFAULT ''
					      ,CLAIM_ADJ_CODE_6                      	VARCHAR	(10)   DEFAULT ''
					      ,CLAIM_ADJ_TYPE_6                      	VARCHAR	(2)    DEFAULT ''
					      ,CLAIM_ADJUSTOR_ID_6                   	VARCHAR	(20)   DEFAULT ''
					      ,CLAIM_ADJ_CODE_7                      	VARCHAR	(10)   DEFAULT ''
					      ,CLAIM_ADJ_TYPE_7                      	VARCHAR	(2)    DEFAULT ''
					      ,CLAIM_ADJUSTOR_ID_7                   	VARCHAR	(20)   DEFAULT ''
					      ,CLAIM_ADJ_CODE_8                      	VARCHAR	(10)   DEFAULT ''
					      ,CLAIM_ADJ_TYPE_8                      	VARCHAR	(2)    DEFAULT ''
					      ,CLAIM_ADJUSTOR_ID_8                   	VARCHAR	(20)   DEFAULT ''
					      ,CLAIM_ADJ_CODE_9                      	VARCHAR	(10)   DEFAULT ''
					      ,CLAIM_ADJ_TYPE_9                      	VARCHAR	(2)    DEFAULT ''
					      ,CLAIM_ADJUSTOR_ID_9                   	VARCHAR	(20)   DEFAULT ''
					      ,CLAIM_ADJ_CODE_10                     	VARCHAR	(10)   DEFAULT ''
					      ,CLAIM_ADJ_TYPE_10                     	VARCHAR	(2)    DEFAULT ''
					      ,CLAIM_ADJUSTOR_ID_10                  	VARCHAR	(20)   DEFAULT ''
					      ,CV_US_ONLY_YN                         	VARCHAR	(1)
					      ,CLAIM_CDF_TEXT_1                      	VARCHAR	(31)   DEFAULT ''
					      ,CLAIM_CDF_TEXT_2                      	VARCHAR	(32)   DEFAULT ''
					      ,CLAIM_CDF_TEXT_3                      	VARCHAR	(32)   DEFAULT ''
					      ,CHECK_NUM                             	VARCHAR	(25)   DEFAULT ''
					      ,CLAIM_FEE_SCHEDULE_AMOUNT             	NUMERIC (19,2)
					      ,CLAIM_REIMBURSE_TYPE                  	VARCHAR	(20)   DEFAULT ''
					      ,PATIENT_PLAN_CODE                     	VARCHAR	(20)   DEFAULT ''
					      ,PER_CASE_RATE_AMOUNT                  	NUMERIC (19,2)
					      ,PER_DIEM_RATE_AMOUNT                  	NUMERIC (19,2)
					      ,PER_DIEM_WEIGHTED_AMOUNT              	NUMERIC (19,2)
					      ,CLIENT_PLATFORM                       	VARCHAR	(20)   DEFAULT ''
					      ,SUB_MEDICARE_ID                       	VARCHAR	(25)   DEFAULT ''
						--2.3 changes new field start
						  ,MEDICAID_ID 								VARCHAR(25)    DEFAULT ''
						  ,PATIENT_DEATH_DATE 						DATE
						  ,LENGTH_OF_STAY 							NUMERIC(4, 0)
						  ,GROUPER_VERSION 							VARCHAR(5) 	   DEFAULT ''
						  ,BLUE_CARD_INDICATOR 						VARCHAR(1) 	   DEFAULT ''
						  ,ITS_HOME_STATE 							VARCHAR(2) 	   DEFAULT ''
						  ,ITS_HOST_STATE 							VARCHAR(2) 	   DEFAULT ''
						  ,ITS_SERIAL_NUM 							VARCHAR(20)    DEFAULT ''
						  ,MEDICARE_PROVIDER_ID 					VARCHAR(80)    DEFAULT ''
						  ,MEDICAID_PROVIDER_ID 					VARCHAR(80)    DEFAULT ''
						  ,RENDERING_TAXONOMY_CODE 					VARCHAR(50)    DEFAULT ''
						  ,BILLING_TAXONOMY_CODE 					VARCHAR(50)    DEFAULT ''
						  ,CLAIM_COINSURANCE_AMOUNT 				NUMERIC(19, 2) --cotiviti NUMERIC(10, 2)
						  ,CLAIM_COPAY_AMOUNT 						NUMERIC(19, 2) --cotiviti NUMERIC(10, 2)
						  ,CLAIM_DEDUCTIBLE_AMOUNT 					NUMERIC(19, 2) --cotiviti NUMERIC(10, 2)
						  ,CLAIM_DISALLOWED_AMOUNT 					NUMERIC(10, 2)
						  ,CLAIM_PATIENT_LIABILITY_AMOUNT 			NUMERIC(10, 2)
						  ,CLAIM_COB_AMOUNT 						NUMERIC(19, 2) --cotiviti NUMERIC(10, 2)
						  ,CLAIM_COB_ALLOWED_AMOUNT 				NUMERIC(19, 2) --cotiviti NUMERIC(10, 2)
						  ,CLAIM_COB_COINSURANCE_AMOUNT 			NUMERIC(19, 2) --cotiviti NUMERIC(10, 2)
						  ,CLAIM_COB_PAID_AMOUNT 					NUMERIC(19, 2) --cotiviti NUMERIC(10, 2)
						  ,CLAIM_COB_DEDUCT_AMOUNT 					NUMERIC(19, 2) --cotiviti NUMERIC(10, 2)
						  ,CLAIM_CHECK_PAID_DATE 					DATE
						  ,PAID_TO_CODE 							VARCHAR(1) 	   DEFAULT ''
						  ,PAYMENT_STATUS 							VARCHAR(20)    DEFAULT ''
						  ,CLAIM_CAPITATION_INDICATOR 				VARCHAR(1)     DEFAULT ''
						  ,CLAIM_FEE_SCHEDULE_CODE 					VARCHAR(25)    DEFAULT ''
						  ,WHOLE_CLAIM_PRICING_LINE_YN 				VARCHAR(1)     DEFAULT ''
						  ,OTHER_INSURANCE_INDICATOR 				VARCHAR(1)     DEFAULT ''
						  ,COORDINATED_BENEFITS_YN 					VARCHAR(1)     DEFAULT ''
						  ,RETIRED_INDICATOR 						VARCHAR(1)     DEFAULT ''
						  ,ATTENDING_TAXONOMY_CODE 					VARCHAR(50)    DEFAULT ''
						  ,OPERATING_TAXONOMY_CODE 					VARCHAR(50)    DEFAULT ''
						  ,OTHER_OPERATING_TAXONOMY_CODE 			VARCHAR(50)    DEFAULT ''
						  ,REFERRING_TAXONOMY_CODE 					VARCHAR(50)    DEFAULT ''
						  ,DOCUMENT_CONTROL_NUMBER 					VARCHAR(20)    DEFAULT ''
						  ,CLAIM_ADJUSTMENT_NUMBER 					VARCHAR(1)     DEFAULT ''
						  ,DATA_SOURCE 								VARCHAR(20)    DEFAULT ''
						  ,PAPER_INDICATOR 							VARCHAR(1)     DEFAULT ''
						  ,CIT_RESTRICTED_ID 						VARCHAR(25)    DEFAULT ''
						--2.3 changes new field end
						  ,ENDORSEMENT_EFF_TIME						DATETIME
						  ,IS_ADJUSTED_CLAIM						VARCHAR(10)
						  ,IS_VOIDED								VARCHAR(10))


          INSERT INTO #INSTITUTIONAL_CLAIM_DETAILS (
                          CLAIM_TYPE_NAME
                         ,CLAIM_ID
                         ,CLAIM_FACT_KEY
						 ,CLAIM_TYPE
						 ,PAYER_SHORT
                         ,SUB_ID
                         ,PATIENT_DOB
                         ,PATIENT_GENDER_ID
                         ,PATIENT_ID
                         ,PATIENT_CONTROL_NUMBER
                         ,PATIENT_LAST_NAME
                         ,PATIENT_FIRST_NAME
                         ,PATIENT_MIDDLE_NAME
                         ,PATIENT_NAME_SUFFIX
                         ,RELATIONSHIP_TO_SUB
                         ,BILL_TYPE
                         ,CLAIM_DOS_FROM
                         ,CLAIM_DOS_TO
                         ,ADMIT_DATE
                         ,DISCHARGE_DATE
                         ,DISCHARGE_STATUS
                         ,SUB_DRG
                         ,ALLOWED_DRG
                         ,PRINCIPAL_DIAGNOSIS_QUAL
                         ,PRINCIPAL_DIAGNOSIS
                         ,PRINCIPAL_DIAGNOSIS_POA
                         ,OTHER_DIAGNOSIS_LIST
                         ,ADMITTING_DIAGNOSIS_QUAL
                         ,ADMITTING_DIAGNOSIS
                         ,EXTERNAL_CAUSE_OF_INJURY_LIST
                         ,PATIENTS_REASON_FOR_VISIT_LIST
                         ,PRINCIPAL_PROCEDURE_QUAL
                         ,PRINCIPAL_PROCEDURE
                         ,PRINCIPAL_PROCEDURE_DATE
                         ,OTHER_PROCEDURE_LIST
                         ,RENDERING_PROVIDER_ID
                         ,RENDERING_TAXGROUP_ID
                         ,RENDERING_NPI
                         ,RENDERING_SUBSPEC_ID
                         ,RENDERING_ENTITY_TYPE
                         ,RENDERING_LAST_NAME
                         ,RENDERING_FIRST_NAME
                         ,RENDERING_MIDDLE_NAME
                         ,RENDERING_NAME_SUFFIX
                         ,RENDERING_STREET_ADDRESS_1
                         ,RENDERING_STREET_ADDRESS_2
                         ,RENDERING_CITY
                         ,RENDERING_STATE
                         ,RENDERING_ZIP
                         ,RENDERING_ZIP_PLUS_4
                         ,RENDERING_COUNTRY
                         ,BILLING_PROVIDER_ID
                         ,BILLING_TAXGROUP_ID
                         ,CV_US_ONLY_YN
						 ,ENDORSEMENT_EFF_TIME
						 ,IS_ADJUSTED_CLAIM
						 ,IS_VOIDED
						 ,PATIENT_SSN--2.3 changes
						 ,SUB_MEDICARE_ID --2.3 changes
						 ,PATIENT_DEATH_DATE--2.3 changes
						 ,LENGTH_OF_STAY --2.3 changes
						 ,MEDICARE_PROVIDER_ID --2.3 changes
						 ,RENDERING_TAXONOMY_CODE --2.3 changes
						 ,BILLING_SUBSPEC_ID --2.3 changes
						 ,BILLING_TAXONOMY_CODE --2.3 changes
						 ,BILLING_ENTITY_TYPE --2.3 changes
						 ,BILLING_NAME --2.3 changes
						 ,BILLING_ADDRESS_1 --2.3 changes
						 ,BILLING_ADDRESS_2 --2.3 changes
						 ,BILLING_CITY --2.3 changes
						 ,BILLING_STATE --2.3 changes
						 ,BILLING_ZIP --2.3 changes
						 ,BILLING_ZIP_PLUS_4 --2.3 changes
						 ,BILLING_COUNTRY --2.3 changes
						 ,ASSIGNMENT_OF_BENEFITS --2.3 changes
						 ,CLAIM_BILLED_AMOUNT--2.3 changes
						,CLAIM_NONCOVERED_AMOUNT--2.3 changes
						,CLAIM_ALLOWED_AMOUNT--2.3 changes
						,CLAIM_PAID_AMOUNT--2.3 changes
						,CLAIM_COINSURANCE_AMOUNT--2.3 changes
						,CLAIM_COPAY_AMOUNT--2.3 changes
						,CLAIM_DEDUCTIBLE_AMOUNT--2.3 changes
						,CLAIM_PATIENT_LIABILITY_AMOUNT--2.3 changes
						,DATE_RECEIVED_CLIENT--2.3 changes
						,DATE_ADJUDICATED --V1.3
						,CLAIM_ID_ORIG--2.3 changes
						,GROUP_ID--2.3 changes
						,GROUP_NAME--2.3 changes
						,SUB_LAST_NAME--2.3 changes
						,SUB_FIRST_NAME--2.3 changes
						,SUB_MIDDLE_NAME--2.3 changes
						,SUB_NAME_SUFFIX--2.3 changes
						,SUB_ADDRESS_1--2.3 changes
						,SUB_ADDDRESS_2--2.3 changes
						,SUB_CITY--2.3 changes
						,SUB_STATE--2.3 changes
						,SUB_ZIP--2.3 changes
						,SUB_ZIP_PLUS_4--2.3 changes
						,SUB_COUNTRY--2.3 changes
						,SUB_DOB--2.3 changes
						,SUB_GENDER_ID--2.3 changes
						,SUB_SSN--2.3 changes
						,PATIENT_ADDRESS_1--2.3 changes
						,PATIENT_ADDRESS_2--2.3 changes
						,PATIENT_CITY--2.3 changes
						,PATIENT_STATE--2.3 changes
						,PATIENT_ZIP--2.3 changes
						,PATIENT_ZIP_PLUS_4--2.3 changes
						,PATIENT_COUNTRY--2.3 changes
						,REFERRING_PROVIDER_ID--2.3 changes
						,REFERRING_NPI--2.3 changes
						,REFERRING_SUBSPEC_ID--2.3 changes
						,REFERRING_TAXONOMY_CODE--2.3 changes
						,REFERRING_LAST_NAME--2.3 changes
						,REFERRING_FIRST_NAME--2.3 changes
						,REFERRING_MIDDLE_NAME--2.3 changes
						,REFERRING_NAME_SUFFIX--2.3 changes
						 )

          SELECT
				ISNULL(UPPER(CLAIM.CLAIM_TYPE_NAME),'') AS CLAIM_TYPE_NAME
				,ISNULL(CLAIM.CLAIM_ID,'') AS CLAIM_ID
				,CLAIM.CLAIM_FACT_KEY
				,CLAIM.BILL_TYPE AS CLAIM_TYPE
				,CLAIM.PAYER_SHORT
				,ISNULL(MEMBER.SUB_ID,'') AS SUB_ID
				,MEMBER.PATIENT_DOB
				,ISNULL(MEMBER.PATIENT_GENDER_ID,'') AS PATIENT_GENDER_ID
				,ISNULL(OTHER_ID.PATIENT_ID,'') AS PATIENT_ID
				,ISNULL(CLAIM.PATIENT_CONTROL_NUMBER,'') AS PATIENT_CONTROL_NUMBER
				,ISNULL(MEMBER.PATIENT_LAST_NAME,'') AS PATIENT_LAST_NAME
				,ISNULL(MEMBER.PATIENT_FIRST_NAME,'') AS PATIENT_FIRST_NAME
				,ISNULL(MEMBER.PATIENT_MIDDLE_NAME,'') AS PATIENT_MIDDLE_NAME
				,ISNULL(MEMBER.PATIENT_NAME_SUFFIX,'') AS PATIENT_NAME_SUFFIX
				,ISNULL(MEMBER.RELATIONSHIP_TO_SUB,'') AS RELATIONSHIP_TO_SUB
				--,ISNULL(CLAIM.BILL_TYPE,'') AS BILL_TYPE -V1.4
				,RIGHT(CONCAT('0000',CLAIM.BILL_TYPE),4) AS BILL_TYPE
				,ISNULL(CLAIM.CLAIM_DOS_FROM,'') AS CLAIM_DOS_FROM
				,ISNULL(CLAIM.CLAIM_DOS_TO,'') AS CLAIM_DOS_TO
				,CLAIM.ADMIT_DATE
				,CLAIM.DISCHARGE_DATE
				,ISNULL(CLAIM.DISCHARGE_STATUS,'') AS DISCHARGE_STATUS
				,ISNULL(DRG.SUB_DRG,'') AS SUB_DRG
				,ISNULL(DRG.ALLOWED_DRG,'') AS ALLOWED_DRG
				,IIF(CLAIM.PRINCIPAL_DIAGNOSIS IS NOT NULL, '0', '') AS PRINCIPAL_DIAGNOSIS_QUAL
				,ISNULL(CLAIM.PRINCIPAL_DIAGNOSIS,'') AS PRINCIPAL_DIAGNOSIS
				,CASE WHEN (CLAIM.PRINCIPAL_DIAGNOSIS_POA) IN ('N/S','N/A') THEN '' ELSE CLAIM.PRINCIPAL_DIAGNOSIS_POA END
				, CONVERT(VARCHAR (MAX),
				(SELECT
					'0' AS DIAGNOSIS_QUAL
					,STANDARDIZED_DIAGNOSIS_CODE
					,DIAGNOSIS_POA_INDICATOR_CODE
					,SORT_ORDER
				 FROM #CLAIM_DIAGNOSIS
				FOR XML PATH ('OTHER_DIAGNOSIS'), ROOT ('OTHER_DIAGNOSIS_LIST'))) AS OTHER_DIAGNOSIS_LIST
				,IIF(CLAIM.ADMITTING_DIAGNOSIS IS NOT NULL, '0', '') AS ADMITTING_DIAGNOSIS_QUAL
				,ISNULL(CLAIM.ADMITTING_DIAGNOSIS,'') AS ADMITTING_DIAGNOSIS
				,CONVERT(VARCHAR (MAX),
				(SELECT
					'0' AS DIAGNOSIS_QUAL
					,STANDARDIZED_DIAGNOSIS_CODE
					,DIAGNOSIS_POA_INDICATOR_CODE
					,ROW_NUMBER() OVER(ORDER BY SORT_ORDER ASC) AS SORT_ORDER
				 FROM #CLAIM_DIAGNOSIS
				 WHERE UPPER(CLAIM_DIAGNOSIS_TYPE)='E'
				FOR XML PATH ('DIAGNOSIS'), ROOT ('EXTERNAL_CAUSE_OF_INJURY_LIST'))) AS EXTERNAL_CAUSE_OF_INJURY_LIST
				,CONVERT(VARCHAR (MAX),
				(SELECT
					'0' AS DIAGNOSIS_QUAL
					,STANDARDIZED_DIAGNOSIS_CODE
					,DIAGNOSIS_POA_INDICATOR_CODE
					,ROW_NUMBER() OVER(ORDER BY SORT_ORDER ASC) AS SORT_ORDER
				 FROM #CLAIM_DIAGNOSIS
				 WHERE UPPER(CLAIM_DIAGNOSIS_TYPE)='R'
				FOR XML PATH ('DIAGNOSIS'), ROOT ('PATIENTS_REASON_FOR_VISIT_LIST'))) AS PATIENTS_REASON_FOR_VISIT_LIST
				,IIF(CLAIM.PRINCIPAL_PROCEDURE IS NOT NULL, '0', '') AS PRINCIPAL_PROCEDURE_QUAL
				,ISNULL(CLAIM.PRINCIPAL_PROCEDURE,'') AS PRINCIPAL_PROCEDURE
				,CLAIM.PRINCIPAL_PROCEDURE_DATE
				,CONVERT(VARCHAR (MAX),
				(SELECT
					'0' AS PROCEDURE_QUAL
					,PROCEDURE_CODE
					,PROCEDURE_CODE_DATE
					,SORT_ORDER
				 FROM #CLAIM_PROCEDURE
				FOR XML PATH ('PROCEDURE'), ROOT ('OTHER_PROCEDURE_LIST'))) AS OTHER_PROCEDURE_LIST
				,ISNULL(RENDERING.RENDERING_PROVIDER_ID,'') AS RENDERING_PROVIDER_ID
				--,ISNULL(RENDERING.RENDERING_TAXGROUP_ID,'') AS RENDERING_TAXGROUP_ID --V1.4
				,COALESCE(RENDERING.RENDERING_TAXGROUP_ID,BILLING.BILLING_TAXGROUP_ID) AS RENDERING_TAXGROUP_ID --V1.4
				,RENDERING.RENDERING_NPI
				,ISNULL(RENDERING.RENDERING_SUBSPEC_ID,'') AS RENDERING_SUBSPEC_ID
				,ISNULL(RENDERING.RENDERING_ENTITY_TYPE,'') AS RENDERING_ENTITY_TYPE
				,ISNULL(RENDERING.RENDERING_LAST_NAME,'') AS RENDERING_LAST_NAME
				,ISNULL(RENDERING.RENDERING_FIRST_NAME,'') AS  RENDERING_FIRST_NAME
				,ISNULL(RENDERING.RENDERING_MIDDLE_NAME,'') AS RENDERING_MIDDLE_NAME
				,ISNULL(RENDERING.RENDERING_NAME_SUFFIX,'') AS RENDERING_NAME_SUFFIX
				,ISNULL(RENDERING.RENDERING_STREET_ADDRESS_1,'') AS RENDERING_STREET_ADDRESS_1
				,ISNULL(RENDERING.RENDERING_STREET_ADDRESS_2,'') AS RENDERING_STREET_ADDRESS_2
				,ISNULL(RENDERING.RENDERING_CITY,'') AS RENDERING_CITY
				,ISNULL(RENDERING.RENDERING_STATE,'') AS RENDERING_STATE
				,ISNULL(RENDERING.RENDERING_ZIP,'') AS RENDERING_ZIP
				,ISNULL(RENDERING.RENDERING_ZIP_PLUS_4,'') AS RENDERING_ZIP_PLUS_4
				,ISNULL(RENDERING.RENDERING_COUNTRY,'') AS RENDERING_COUNTRY
				,ISNULL(BILLING.BILLING_PROVIDER_ID,'') AS BILLING_PROVIDER_ID
				,REPLACE(ISNULL(BILLING.BILLING_TAXGROUP_ID,''), '-', '') AS BILLING_TAXGROUP_ID  --V1.3
				,CLAIM.CV_US_ONLY_YN
				,CLAIM.ENDORSEMENT_EFF_TIME
				,CLAIM.IS_ADJUSTED_CLAIM
				,CLAIM.IS_VOIDED
				,REPLACE(ISNULL(MEMBER.PATIENT_SSN,''), '-','') AS PATIENT_SSN  --2.3 changes --V1.3
				,ISNULL(CLAIM.SUB_MEDICARE_ID,'') AS SUB_MEDICARE_ID --2.3 changes
				,MEMBER.PATIENT_DEATH_DATE--2.3 changes
				,CLAIM.LENGTH_OF_STAY AS LENGTH_OF_STAY --2.3 changes
				,ISNULL(SPPLR_OTHER_ID.MEDICARE_PROVIDER_ID,'') AS MEDICARE_PROVIDER_ID --2.3 changes
				,ISNULL(RENDERING.RENDERING_SUBSPEC_ID,'') AS RENDERING_TAXONOMY_CODE --2.3 changes
				,ISNULL(BILLING.BILLING_SUBSPEC_ID,'') AS BILLING_SUBSPEC_ID --2.3 changes
				,ISNULL(BILLING.BILLING_SUBSPEC_ID,'') AS BILLING_TAXONOMY_CODE --2.3 changes
				,BILLING.BILLING_ENTITY_TYPE AS BILLING_ENTITY_TYPE --2.3 changes
				,BILLING.BILLING_LAST_NAME AS BILLING_NAME --2.3 changes
				,BILLING.BILLING_STREET_ADDRESS_1 --2.3 changes
				,BILLING.BILLING_STREET_ADDRESS_2 --2.3 changes
				,BILLING.BILLING_CITY --2.3 changes
				,BILLING.BILLING_STATE --2.3 changes
				,BILLING.BILLING_ZIP --2.3 changes
				,BILLING.BILLING_ZIP_PLUS_4 --2.3 changes
				,BILLING.BILLING_COUNTRY --2.3 changes
				,ISNULL(CLAIM.ASSIGNMENT_OF_BENEFITS,'') AS ASSIGNMENT_OF_BENEFITS --2.3 changes
				,CLAIM_AMT.CLAIM_BILLED_AMOUNT--2.3 changes
				,CLAIM_AMT.CLAIM_NONCOVERED_AMOUNT--2.3 changes
				,CLAIM_AMT.CLAIM_ALLOWED_AMOUNT--2.3 changes
				,CLAIM_AMT.CLAIM_PAID_AMOUNT--2.3 changes
				,CLAIM_COINSURANCE_AMOUNT--2.3 changes
				,CLAIM_COPAY_AMOUNT--2.3 changes
				,CLAIM_DEDUCTIBLE_AMOUNT--2.3 changes
				,CLAIM_PATIENT_LIABILITY_AMOUNT--2.3 changes
				,CLAIM.DATE_RECEIVED_CLIENT --2.3 changes
				,CLAIM.DATE_ADJUDICATED --V1.3
				,ISNULL(CLAIM.CLAIM_ID_ORIG,'') AS CLAIM_ID_ORIG--2.3 changes
				,ISNULL(MEMBER.GROUP_ID,'') AS GROUP_ID--2.3 changes
				,ISNULL(MEMBER.GROUP_NAME,'') AS GROUP_NAME --2.3 changes
				,ISNULL(MEMBER.SUB_LAST_NAME,'') AS SUB_LAST_NAME--2.3 changes
				,ISNULL(MEMBER.SUB_FIRST_NAME,'') AS SUB_FIRST_NAME--2.3 changes
				,ISNULL(MEMBER.SUB_MIDDLE_NAME,'') AS SUB_MIDDLE_NAME--2.3 changes
				,ISNULL(MEMBER.SUB_NAME_SUFFIX,'') AS SUB_NAME_SUFFIX--2.3 changes
				,ISNULL(MEMBER.PATIENT_ADDRESS_1,'') AS SUB_ADDRESS_1 --2.3 changes
				,ISNULL(MEMBER.PATIENT_ADDRESS_2,'') AS SUB_ADDDRESS_2--2.3 changes
				,ISNULL(MEMBER.PATIENT_CITY,'')  AS SUB_CITY--2.3 changes
				,ISNULL(MEMBER.PATIENT_STATE,'') AS SUB_STATE--2.3 changes
				,ISNULL(MEMBER.PATIENT_ZIP,'') AS SUB_ZIP--2.3 changes
				,ISNULL(MEMBER.PATIENT_ZIP_PLUS_4,'') AS SUB_ZIP_PLUS_4--2.3 changes
				,ISNULL(MEMBER.PATIENT_COUNTRY,'') AS SUB_COUNTRY--2.3 changes
				,MEMBER.PATIENT_DOB AS SUB_DOB--2.3 changes
				,ISNULL(MEMBER.PATIENT_GENDER_ID,'') AS SUB_GENDER_ID--2.3 changes
				,REPLACE(ISNULL(MEMBER.PATIENT_SSN,''), '-', '') AS SUB_SSN--2.3 changes --V1.3
				,ISNULL(MEMBER.PATIENT_ADDRESS_1,'') AS PATIENT_ADDRESS_1--2.3 changes
				,ISNULL(MEMBER.PATIENT_ADDRESS_2,'') AS PATIENT_ADDRESS_2--2.3 changes
				,ISNULL(MEMBER.PATIENT_CITY,'') AS PATIENT_CITY--2.3 changes
				,ISNULL(MEMBER.PATIENT_STATE,'') AS PATIENT_STATE--2.3 changes
				,ISNULL(MEMBER.PATIENT_ZIP,'') AS PATIENT_ZIP--2.3 changes
				,ISNULL(MEMBER.PATIENT_ZIP_PLUS_4,'') AS PATIENT_ZIP_PLUS_4--2.3 changes
				,ISNULL(MEMBER.PATIENT_COUNTRY,'') AS PATIENT_COUNTRY--2.3 changes
				,REFR.PRACTITIONER_HCC_ID    AS REFERRING_PROVIDER_ID --2.3 changes
				,REFR.PRACTITIONER_NPI       AS REFERRING_NPI--2.3 changes
				,REFR.PROVIDER_TAXONOMY_CODE AS REFERRING_SUBSPEC_ID--2.3 changes
				,REFR.PROVIDER_TAXONOMY_CODE AS REFERRING_TAXONOMY_CODE--2.3 changes
				,REFR.PRACTITIONER_LAST_NAME AS REFERRING_LAST_NAME--2.3 changes
				,REFR.PRACTITIONER_FIRST_NAME  AS REFERRING_FIRST_NAME--2.3 changes
				,REFR.PRACTITIONER_MIDDLE_NAME AS REFERRING_MIDDLE_NAME--2.3 changes
				,REFR.PRACTITIONER_NAME_SUFFIX AS REFERRING_NAME_SUFFIX--2.3 changes
          FROM #CLAIM_DETAILS CLAIM
          LEFT JOIN #CLAIM_DRG DRG
          ON CLAIM.CLAIM_FACT_KEY = DRG.CLAIM_FACT_KEY
          LEFT JOIN #MEMBER_DETAILS MEMBER
          ON CLAIM.CLAIM_FACT_KEY = MEMBER.CLAIM_FACT_KEY
          LEFT JOIN #MEMBER_OTHER_ID OTHER_ID
          ON CLAIM.CLAIM_FACT_KEY = OTHER_ID.CLAIM_FACT_KEY
          LEFT JOIN #BILLING_PROV_DETAILS BILLING
          ON CLAIM.CLAIM_FACT_KEY = BILLING.CLAIM_FACT_KEY
          LEFT JOIN #RENDERING_PROV_DETAILS RENDERING
          ON CLAIM.CLAIM_FACT_KEY = RENDERING.CLAIM_FACT_KEY
		  LEFT JOIN #SUPPLIER_OTHER_ID SPPLR_OTHER_ID --2.3 changes
          ON CLAIM.CLAIM_FACT_KEY = SPPLR_OTHER_ID.CLAIM_FACT_KEY --2.3 changes
          LEFT JOIN #CLAIM_AMOUNTS CLAIM_AMT--2.3 changes
          ON CLAIM.CLAIM_FACT_KEY = CLAIM_AMT.CLAIM_FACT_KEY   --2.3 changes
     	  LEFT JOIN #REFERING_TEMP REFR --2.3 changes
		  ON REFR.REFERRING_PRACTITIONER_KEY = CLAIM.REFERRING_PRACTITIONER_KEY--2.3 changes

	  SELECT * FROM #INSTITUTIONAL_CLAIM_DETAILS

      END
      ELSE IF UPPER(@lv_claim_type_name)='PROFESSIONAL'
      BEGIN

        CREATE TABLE #PROFESSIONAL_CLAIM_DETAILS (
						   CLAIM_TYPE_NAME                      VARCHAR	(20)
					      ,CLAIM_ID                            	VARCHAR	(25)
					      ,CLAIM_FACT_KEY                      	NUMERIC	(19,0)
					      ,CLAIM_TYPE                          	VARCHAR	(1)
						  ,PAYER_SHORT                          VARCHAR (5)
					      ,SUB_ID                              	VARCHAR	(25)
					      ,DEP_ID                              	VARCHAR	(10)        DEFAULT ''
					      ,PATIENT_DOB                         	DATE
					      ,PATIENT_GENDER_ID                   	VARCHAR	(10)
					      ,PATIENT_SSN                         	VARCHAR	(9)
					      ,PATIENT_ID                          	VARCHAR	(20)
					      ,PATIENT_CONTROL_NUMBER             	VARCHAR	(24)
					      ,PATIENT_LAST_NAME                   	VARCHAR	(60)
					      ,PATIENT_FIRST_NAME                  	VARCHAR	(35)
					      ,PATIENT_MIDDLE_NAME                 	VARCHAR	(25)
					      ,PATIENT_NAME_SUFFIX                 	VARCHAR	(10)
					      ,RELATIONSHIP_TO_SUB                 	VARCHAR	(2)
					      ,PATIENT_DEATH_DATE                  	DATE
					      ,PATIENT_WEIGHT                      	NUMERIC	(6,0)
					      ,PREGNANCY_INDICATOR_YN              	VARCHAR	(1)          DEFAULT ''
					      ,MED_REC_NO                          	VARCHAR	(24)         DEFAULT ''
					      ,DIAGNOSIS_CODE_LIST                 	VARCHAR	(MAX)
					      ,DATE_RECEIVED_CLIENT                	DATE
					      ,ASSIGNMENT_OF_BENEFITS              	VARCHAR	(1)
					      ,CLAIM_BILLED_AMOUNT                 	NUMERIC (19,2)
					      ,CLAIM_NONCOVERED_AMOUNT             	NUMERIC (19,2)
					      ,CLAIM_ALLOWED_AMOUNT                	NUMERIC (19,2)
					      ,CLAIM_PAID_AMOUNT                   	NUMERIC (19,2)
					      ,CLAIM_PAID_DATE                     	DATE
					      ,CLAIM_ID_ORIG                       	VARCHAR	(25)
					      ,GROUP_ID                            	VARCHAR	(15)
					      ,RISK_POOL                           	VARCHAR	(60)          DEFAULT ''
					      ,CLAIM_FILING_INDICATOR              	VARCHAR	(2)           DEFAULT ''
					      ,SUB_LAST_NAME                       	VARCHAR	(60)
					      ,SUB_FIRST_NAME                      	VARCHAR	(35)
					      ,SUB_MIDDLE_NAME                     	VARCHAR	(25)
					      ,SUB_NAME_SUFFIX                     	VARCHAR	(10)
					      ,SUB_ADDRESS_1                       	VARCHAR	(55)
					      ,SUB_ADDDRESS_2                      	VARCHAR	(55)
					      ,SUB_CITY                            	VARCHAR	(30)
					      ,SUB_STATE                           	VARCHAR	(2)
					      ,SUB_ZIP                             	VARCHAR	(5)
					      ,SUB_ZIP_PLUS_4                      	VARCHAR	(4)
					      ,SUB_COUNTRY                         	VARCHAR	(3)
					      ,SUB_COUNTRY_SUBDIVISION             	VARCHAR	(3)           DEFAULT ''
					      ,SUB_DOB                             	DATE
					      ,SUB_GENDER_ID                       	VARCHAR	(10)
					      ,SUB_SSN                             	VARCHAR	(9)
					      ,PATIENT_ADDRESS_1                   	VARCHAR	(55)
					      ,PATIENT_ADDRESS_2                   	VARCHAR	(55)
					      ,PATIENT_CITY                        	VARCHAR	(30)
					      ,PATIENT_STATE                       	VARCHAR	(2)
					      ,PATIENT_ZIP                         	VARCHAR	(5)
					      ,PATIENT_ZIP_PLUS_4                  	VARCHAR	(4)
					      ,PATIENT_COUNTRY                     	VARCHAR	(3)
					      ,PATIENT_COUNTRY_SUBDIVISION         	VARCHAR	(3)           DEFAULT ''
					      ,CLAIM_ATTACHMENT_TYPE_CODE          	VARCHAR	(2)           DEFAULT ''
					      ,CLAIM_ATTACHMENT_TRANSMIT_CODE      	VARCHAR	(2)           DEFAULT ''
					      ,CLAIM_ACN                           	VARCHAR	(80)          DEFAULT ''
					      ,AUTH_EXCEPTION_CODE                 	VARCHAR	(4)           DEFAULT ''
					      ,REFERRAL_ID                         	VARCHAR	(30)          DEFAULT ''
					      ,AUTH_NO_A                           	VARCHAR	(30)          DEFAULT ''
					      ,CLAIM_NOTE_REF_CODE                 	VARCHAR	(3)           DEFAULT ''
					      ,CLAIM_NOTE_TEXT                     	VARCHAR	(80)          DEFAULT ''
					      ,MEDICARE_CROSSOVER_INDICATOR_YN     	VARCHAR	(1)           DEFAULT ''
					      ,EPSDT_REFERRAL_YN                   	VARCHAR	(1)           DEFAULT ''
					      ,EPSDT_CONDITION_CODE_1              	VARCHAR	(3)           DEFAULT ''
					      ,EPSDT_CONDITION_CODE_2              	VARCHAR	(3)           DEFAULT ''
					      ,EPSDT_CONDITION_CODE_3              	VARCHAR	(3)           DEFAULT ''
					      ,EPSDT_INDICATOR_YN                  	VARCHAR	(1)           DEFAULT ''
					      ,ANESTHESIA_RELATED_SURG_HCPCS_1     	VARCHAR	(5)           DEFAULT ''
					      ,ANESTHESIA_RELATED_SURG_HCPCS_2     	VARCHAR	(5)           DEFAULT ''
					      ,ONSET_CURRENT_ILLNESS               	DATE
					      ,INITIAL_TREATMENT_DATE              	DATE
					      ,LAST_SEEN_DATE                      	DATE
					      ,ACUTE_MANISFESTATION_DATE           	DATE
					      ,ACCIDENT_DATE                       	DATE
					      ,LMP_DATE                            	DATE
					      ,ADMIT_DATE                          	DATE
					      ,DISCHARGE_DATE                      	DATE
					      ,ASSUMED_CARE_DATE                   	DATE
					      ,RELINQUISHED_CARE_DATE              	DATE
					      ,CLAIM_FREQUENCY_CODE                	VARCHAR	(1)           DEFAULT ''
					      ,CLAIM_ADJ_CODE_1                    	VARCHAR	(10)          DEFAULT ''
					      ,CLAIM_ADJ_TYPE_1                    	VARCHAR	(2)           DEFAULT ''
					      ,CLAIM_ADJUSTOR_ID_1                 	VARCHAR	(20)          DEFAULT ''
					      ,CLAIM_ADJ_CODE_2                    	VARCHAR	(10)          DEFAULT ''
					      ,CLAIM_ADJ_TYPE_2                    	VARCHAR	(2)           DEFAULT ''
					      ,CLAIM_ADJUSTOR_ID_2                 	VARCHAR	(20)          DEFAULT ''
					      ,CLAIM_ADJ_CODE_3                    	VARCHAR	(10)          DEFAULT ''
					      ,CLAIM_ADJ_TYPE_3                    	VARCHAR	(2)           DEFAULT ''
					      ,CLAIM_ADJUSTOR_ID_3                 	VARCHAR	(20)          DEFAULT ''
					      ,CLAIM_ADJ_CODE_4                    	VARCHAR	(10)          DEFAULT ''
					      ,CLAIM_ADJ_TYPE_4                    	VARCHAR	(2)           DEFAULT ''
					      ,CLAIM_ADJUSTOR_ID_4                 	VARCHAR	(20)          DEFAULT ''
					      ,CLAIM_ADJ_CODE_5                    	VARCHAR	(10)          DEFAULT ''
					      ,CLAIM_ADJ_TYPE_5                    	VARCHAR	(2)           DEFAULT ''
					      ,CLAIM_ADJUSTOR_ID_5                 	VARCHAR	(20)          DEFAULT ''
					      ,CLAIM_ADJ_CODE_6                    	VARCHAR	(10)          DEFAULT ''
					      ,CLAIM_ADJ_TYPE_6                    	VARCHAR	(2)           DEFAULT ''
					      ,CLAIM_ADJUSTOR_ID_6                 	VARCHAR	(20)          DEFAULT ''
					      ,CLAIM_ADJ_CODE_7                    	VARCHAR	(10)          DEFAULT ''
					      ,CLAIM_ADJ_TYPE_7                    	VARCHAR	(2)           DEFAULT ''
					      ,CLAIM_ADJUSTOR_ID_7                 	VARCHAR	(20)          DEFAULT ''
					      ,CLAIM_ADJ_CODE_8                    	VARCHAR	(10)          DEFAULT ''
					      ,CLAIM_ADJ_TYPE_8                    	VARCHAR	(2)           DEFAULT ''
					      ,CLAIM_ADJUSTOR_ID_8                 	VARCHAR	(20)          DEFAULT ''
					      ,CLAIM_ADJ_CODE_9                    	VARCHAR	(10)          DEFAULT ''
					      ,CLAIM_ADJ_TYPE_9                    	VARCHAR	(2)           DEFAULT ''
					      ,CLAIM_ADJUSTOR_ID_9                 	VARCHAR	(20)          DEFAULT ''
					      ,CLAIM_ADJ_CODE_10                   	VARCHAR	(10)          DEFAULT ''
					      ,CLAIM_ADJ_TYPE_10                   	VARCHAR	(2)           DEFAULT ''
					      ,CLAIM_ADJUSTOR_ID_10                	VARCHAR	(20)          DEFAULT ''
					      ,CV_US_ONLY_YN                       	VARCHAR	(1)
					      ,CLAIM_CDF_TEXT_1                    	VARCHAR	(32)          DEFAULT ''--older [varchar](31)
					      ,CLAIM_CDF_TEXT_2                    	VARCHAR	(32)          DEFAULT ''
					      ,CLAIM_CDF_TEXT_3                    	VARCHAR	(32)          DEFAULT ''
						  --2.3 Changes new fields start
						  ,GROUP_NAME 							VARCHAR (60) 		  DEFAULT ''--moved to claim level from line level in cotiviti 2.3
						  ,CHIRO_LAST_XRAY_DATE 				DATE --moved to claim level from line level in cotiviti 2.3
						  ,FORM_ID_CODE							VARCHAR (3) 		  DEFAULT ''--moved to claim level from line level in cotiviti 2.3
						  ,FORM_ID 								VARCHAR(30) 		  DEFAULT ''--moved to claim level from line level in cotiviti 2.3
						  ,SUB_MEDICARE_ID 						VARCHAR(50) 		  DEFAULT ''--cotiviti [varchar](25)
						  ,MEDICAID_ID							VARCHAR(25) 		  DEFAULT ''
						  ,BLUE_CARD_INDICATOR 					VARCHAR(1) 			  DEFAULT ''
						  ,ITS_HOME_STATE 						VARCHAR(2) 			  DEFAULT ''
						  ,ITS_HOST_STATE 						VARCHAR(2) 			  DEFAULT ''
						  ,ITS_SERIAL_NUM 						VARCHAR(20) 		  DEFAULT ''
						  ,CLAIM_COINSURANCE_AMOUNT 			NUMERIC(19, 2) 		  --cotiviti numeric(10, 2)
						  ,CLAIM_COPAY_AMOUNT 					NUMERIC(19, 2) 		  --cotiviti numeric(10, 2)
						  ,CLAIM_DEDUCTIBLE_AMOUNT 				NUMERIC(19, 2) 		  --cotiviti numeric(10, 2)
						  ,CLAIM_DISALLOWED_AMOUNT 				NUMERIC(10, 2)
						  ,CLAIM_PATIENT_LIABILITY_AMOUNT 		NUMERIC(10, 2)
						  ,CLAIM_COB_AMOUNT 					NUMERIC(19, 2) 		  --cotiviti numeric(10, 2)
						  ,CLAIM_COB_ALLOWED_AMOUNT 			NUMERIC(19, 2) 		  --cotiviti numeric(10, 2)
						  ,CLAIM_COB_COINSURANCE_AMOUNT 		NUMERIC(19, 2) 		  --cotiviti numeric(10, 2)
						  ,CLAIM_COB_PAID_AMOUNT 				NUMERIC(19, 2) 		  --cotiviti numeric(10, 2)
						  ,CLAIM_COB_DEDUCT_AMOUNT 				NUMERIC(19, 2) 		  --cotiviti numeric(10, 2)
						  ,CLAIM_CHECK_PAID_DATE 				DATE
						  ,PAID_TO_CODE 						VARCHAR(1) 	   		  DEFAULT ''
						  ,PAYMENT_STATUS 						VARCHAR(20)    		  DEFAULT ''
						  ,CLAIM_CAPITATION_INDICATOR 			VARCHAR(1)     		  DEFAULT ''
						  ,CLAIM_FEE_SCHEDULE_CODE 				VARCHAR(25) 		  DEFAULT ''
						  ,PATIENT_PLAN_CODE 					VARCHAR(20) 		  DEFAULT ''
						  ,WHOLE_CLAIM_PRICING_LINE_YN 			VARCHAR(1) 			  DEFAULT ''
						  ,OTHER_INSURANCE_INDICATOR 			VARCHAR(1) 			  DEFAULT ''
						  ,COORDINATED_BENEFITS_YN 				VARCHAR(1) 			  DEFAULT ''
						  ,RETIRED_INDICATOR 					VARCHAR(1) 			  DEFAULT ''
						  ,DOCUMENT_CONTROL_NUMBER 				VARCHAR(20) 		  DEFAULT ''
						  ,CLAIM_ADJUSTMENT_NUMBER 				VARCHAR(1) 			  DEFAULT ''
						  ,CHECK_NUM 							VARCHAR(25) 		  DEFAULT ''
						  ,PLATFORM_CODE 						VARCHAR(20) 		  DEFAULT ''
						  ,DATA_SOURCE 							VARCHAR(20) 		  DEFAULT ''
						  ,PAPER_INDICATOR 						VARCHAR(1) 			  DEFAULT ''
						  ,CIT_RESTRICTED_ID 					VARCHAR(25) 		  DEFAULT ''
						  --2.3 Changes new fields end
						  ,ENDORSEMENT_EFF_TIME					DATETIME
						  ,IS_ADJUSTED_CLAIM					VARCHAR(10)
						  ,IS_VOIDED							VARCHAR(10))


          INSERT INTO #PROFESSIONAL_CLAIM_DETAILS (
                        CLAIM_TYPE_NAME
                       ,CLAIM_ID
                       ,CLAIM_FACT_KEY
                       ,PAYER_SHORT
                       ,CLAIM_TYPE
                       ,SUB_ID
                       ,PATIENT_DOB
                       ,PATIENT_GENDER_ID
                       ,PATIENT_SSN
                       ,PATIENT_ID
                       ,PATIENT_CONTROL_NUMBER
                       ,PATIENT_LAST_NAME
                       ,PATIENT_FIRST_NAME
                       ,PATIENT_MIDDLE_NAME
                       ,PATIENT_NAME_SUFFIX
                       ,RELATIONSHIP_TO_SUB
                       ,PATIENT_DEATH_DATE
                       ,DIAGNOSIS_CODE_LIST
                       ,DATE_RECEIVED_CLIENT
                       ,ASSIGNMENT_OF_BENEFITS
                       ,CLAIM_BILLED_AMOUNT
                       ,CLAIM_NONCOVERED_AMOUNT
                       ,CLAIM_ALLOWED_AMOUNT
                       ,CLAIM_PAID_AMOUNT
                       ,CLAIM_PAID_DATE
                       ,CLAIM_ID_ORIG
                       ,GROUP_ID
                       ,SUB_LAST_NAME
                       ,SUB_FIRST_NAME
                       ,SUB_MIDDLE_NAME
                       ,SUB_NAME_SUFFIX
                       ,SUB_ADDRESS_1
                       ,SUB_ADDDRESS_2
                       ,SUB_CITY
                       ,SUB_STATE
                       ,SUB_ZIP
                       ,SUB_ZIP_PLUS_4
                       ,SUB_COUNTRY
                       ,SUB_DOB
                       ,SUB_GENDER_ID
                       ,SUB_SSN
                       ,PATIENT_ADDRESS_1
                       ,PATIENT_ADDRESS_2
                       ,PATIENT_CITY
                       ,PATIENT_STATE
                       ,PATIENT_ZIP
                       ,PATIENT_ZIP_PLUS_4
                       ,PATIENT_COUNTRY
                       ,ADMIT_DATE
					   ,DISCHARGE_DATE    --2.3 changes
                       ,CV_US_ONLY_YN
					   ,ENDORSEMENT_EFF_TIME
               		   ,IS_ADJUSTED_CLAIM
					   ,IS_VOIDED
					   ,GROUP_NAME--2.3 changes
					   ,SUB_MEDICARE_ID--2.3 changes
					   ,CLAIM_COINSURANCE_AMOUNT--2.3 changes
					   ,CLAIM_COPAY_AMOUNT--2.3 changes
					   ,CLAIM_DEDUCTIBLE_AMOUNT--2.3 changes
					   ,CLAIM_PATIENT_LIABILITY_AMOUNT--2.3 changes
					   )

		  SELECT
              ISNULL(UPPER(CLAIM.CLAIM_TYPE_NAME),'') AS CLAIM_TYPE_NAME
             ,ISNULL(CLAIM.CLAIM_ID,'')
             ,CLAIM.CLAIM_FACT_KEY
             ,CLAIM.PAYER_SHORT
             ,ISNULL(CASE WHEN BILLING.BILLING_SUBSPEC_ID='261QA1903X' THEN 'A' ELSE 'P' END,'') AS CLAIM_TYPE
             ,ISNULL(MEMBER.SUB_ID,'') AS SUB_ID
             ,MEMBER.PATIENT_DOB
             ,ISNULL(MEMBER.PATIENT_GENDER_ID,'') AS PATIENT_GENDER_ID
             ,REPLACE(ISNULL(MEMBER.PATIENT_SSN,''), '-', '') AS PATIENT_SSN  --V1.3
             ,ISNULL(OTHER_ID.PATIENT_ID,'') AS PATIENT_ID
             ,ISNULL(CLAIM.PATIENT_CONTROL_NUMBER,'') AS PATIENT_CONTROL_NUMBER
             ,ISNULL(MEMBER.PATIENT_LAST_NAME,'') AS PATIENT_LAST_NAME
             ,ISNULL(MEMBER.PATIENT_FIRST_NAME,'') AS PATIENT_FIRST_NAME
             ,ISNULL(MEMBER.PATIENT_MIDDLE_NAME,'') AS PATIENT_MIDDLE_NAME
             ,ISNULL(MEMBER.PATIENT_NAME_SUFFIX,'') AS PATIENT_NAME_SUFFIX
             ,ISNULL(MEMBER.RELATIONSHIP_TO_SUB,'') AS RELATIONSHIP_TO_SUB
             ,MEMBER.PATIENT_DEATH_DATE
             ,CONVERT(VARCHAR (MAX),
                (SELECT
                    '0' AS DIAGNOSIS_QUAL
                    ,STANDARDIZED_DIAGNOSIS_CODE
                    ,SORT_ORDER
                 FROM #CLAIM_DIAGNOSIS
                FOR XML PATH ('DIAGNOSIS'), ROOT ('DIAGNOSIS_LIST'))) AS DIAGNOSIS_CODE_LIST
             ,CLAIM.DATE_RECEIVED_CLIENT
             ,ISNULL(CLAIM.ASSIGNMENT_OF_BENEFITS,'') AS ASSIGNMENT_OF_BENEFITS
             ,CLAIM_AMT.CLAIM_BILLED_AMOUNT
             ,CLAIM_AMT.CLAIM_NONCOVERED_AMOUNT
             ,CLAIM_AMT.CLAIM_ALLOWED_AMOUNT
             ,CLAIM_AMT.CLAIM_PAID_AMOUNT
             ,PAYMENT.CLAIM_PAID_DATE
             ,ISNULL(CLAIM.CLAIM_ID_ORIG,'') AS CLAIM_ID_ORIG
             ,ISNULL(MEMBER.GROUP_ID,'') AS GROUP_ID
             ,ISNULL(MEMBER.SUB_LAST_NAME,'') AS SUB_LAST_NAME
             ,ISNULL(MEMBER.SUB_FIRST_NAME,'') AS SUB_FIRST_NAME
             ,ISNULL(MEMBER.SUB_MIDDLE_NAME,'') AS SUB_MIDDLE_NAME
             ,ISNULL(MEMBER.SUB_NAME_SUFFIX,'') AS SUB_NAME_SUFFIX
             ,ISNULL(MEMBER.PATIENT_ADDRESS_1,'') AS SUB_ADDRESS_1 --Patient and subscriber are same for medicaid
             ,ISNULL(MEMBER.PATIENT_ADDRESS_2,'') AS SUB_ADDDRESS_2
             ,ISNULL(MEMBER.PATIENT_CITY,'')  AS SUB_CITY
             ,ISNULL(MEMBER.PATIENT_STATE,'') AS SUB_STATE
             ,ISNULL(MEMBER.PATIENT_ZIP,'') AS SUB_ZIP
             ,ISNULL(MEMBER.PATIENT_ZIP_PLUS_4,'') AS SUB_ZIP_PLUS_4
             ,ISNULL(MEMBER.PATIENT_COUNTRY,'') AS SUB_COUNTRY
             ,MEMBER.PATIENT_DOB AS SUB_DOB
             ,ISNULL(MEMBER.PATIENT_GENDER_ID,'') AS SUB_GENDER_ID
             ,REPLACE(ISNULL(MEMBER.PATIENT_SSN,''), '-', '') AS SUB_SSN --V1.3
             ,ISNULL(MEMBER.PATIENT_ADDRESS_1,'') AS PATIENT_ADDRESS_1
             ,ISNULL(MEMBER.PATIENT_ADDRESS_2,'') AS PATIENT_ADDRESS_2
             ,ISNULL(MEMBER.PATIENT_CITY,'') AS PATIENT_CITY
             ,ISNULL(MEMBER.PATIENT_STATE,'') AS PATIENT_STATE
             ,ISNULL(MEMBER.PATIENT_ZIP,'') AS PATIENT_ZIP
             ,ISNULL(MEMBER.PATIENT_ZIP_PLUS_4,'') AS PATIENT_ZIP_PLUS_4
             ,ISNULL(MEMBER.PATIENT_COUNTRY,'') AS PATIENT_COUNTRY
             ,CLAIM.ADMIT_DATE
			 ,CLAIM.DISCHARGE_DATE--2.3 changes
             ,CLAIM.CV_US_ONLY_YN
			 ,CLAIM.ENDORSEMENT_EFF_TIME
			 ,CLAIM.IS_ADJUSTED_CLAIM
			 ,CLAIM.IS_VOIDED
			 ,ISNULL(MEMBER.GROUP_NAME,'') AS GROUP_NAME --2.3 changes
			 ,ISNULL(CLAIM.SUB_MEDICARE_ID,'') AS SUB_MEDICARE_ID --2.3 changes
			 ,CLAIM_AMT.CLAIM_COINSURANCE_AMOUNT--2.3 changes
             ,CLAIM_AMT.CLAIM_COPAY_AMOUNT--2.3 changes
             ,CLAIM_AMT.CLAIM_DEDUCTIBLE_AMOUNT--2.3 changes
             ,CLAIM_AMT.CLAIM_PATIENT_LIABILITY_AMOUNT--2.3 changes
          FROM #CLAIM_DETAILS CLAIM
          LEFT JOIN #CLAIM_DRG DRG
          ON CLAIM.CLAIM_FACT_KEY = DRG.CLAIM_FACT_KEY
          LEFT JOIN #PAYMENT PAYMENT
          ON CLAIM.CLAIM_FACT_KEY=PAYMENT.CLAIM_FACT_KEY
          LEFT JOIN #MEMBER_DETAILS MEMBER
          ON CLAIM.CLAIM_FACT_KEY = MEMBER.CLAIM_FACT_KEY
          LEFT JOIN #MEMBER_OTHER_ID OTHER_ID
          ON CLAIM.CLAIM_FACT_KEY = OTHER_ID.CLAIM_FACT_KEY
          LEFT JOIN #BILLING_PROV_DETAILS BILLING
          ON CLAIM.CLAIM_FACT_KEY = BILLING.CLAIM_FACT_KEY
          LEFT JOIN #RENDERING_PROV_DETAILS RENDERING
          ON CLAIM.CLAIM_FACT_KEY = RENDERING.CLAIM_FACT_KEY
          LEFT JOIN #CLAIM_AMOUNTS CLAIM_AMT
          ON CLAIM.CLAIM_FACT_KEY = CLAIM_AMT.CLAIM_FACT_KEY


      SELECT * FROM #PROFESSIONAL_CLAIM_DETAILS

      END
       -- DEBUG--  PRINT ' INSERT CLAIM DETAILS INTO THE TEMP TABLE Completed : ' + CONVERT( varchar, Getdate(),121)

       -- DEBUG--  PRINT ' DROPPING TEMPERORY TABLES Started : ' + CONVERT( varchar, Getdate(),121)

            DROP TABLE IF EXISTS #INSTITUTIONAL_CLAIM_DETAILS
            DROP TABLE IF EXISTS #PROFESSIONAL_CLAIM_DETAILS
            DROP TABLE IF EXISTS #CLAIM_DETAILS
            DROP TABLE IF EXISTS #MEMBER_DETAILS
            DROP TABLE IF EXISTS #PAYMENT
            DROP TABLE IF EXISTS #CLAIM_DRG
            DROP TABLE IF EXISTS #MEMBER_OTHER_ID
            DROP TABLE IF EXISTS #CLAIM_DIAGNOSIS
            DROP TABLE IF EXISTS #CLAIM_PROCEDURE
            DROP TABLE IF EXISTS #BILLING_PROV_DETAILS
            DROP TABLE IF EXISTS #RENDERING_PROV_DETAILS
            DROP TABLE IF EXISTS #CLAIM_AMOUNTS
            DROP TABLE IF EXISTS #CLAIM_DETAILS
            DROP TABLE IF EXISTS #MEMBER_DETAILS
			DROP TABLE IF EXISTS #REFERING_TEMP
			DROP TABLE IF EXISTS #SUPPLIER_OTHER_ID --2.3 changes

       -- DEBUG--  PRINT ' DROPPING TEMPERORY TABLES Completed : ' + CONVERT( varchar, Getdate(),121)

  END TRY
  BEGIN CATCH

  SELECT
               @lv_return_code = @@ERROR
            IF @lv_return_code <> 0
                BEGIN
                    SELECT
                        @lv_Msg_Desc            = 'Error Encountered : ' + CAST(Error_Number() AS VARCHAR) + ' : ' + Error_Message()
                                                   + ' at Line ' + CAST(Error_Line() AS VARCHAR) + ' in sp : ' + Error_Procedure(),
						            @lv_Msg_Type    		= 'ERR',
						            @ldt_Log_DTM    		= GETDATE()
				PRINT 	@lv_Msg_Desc
                END

    END CATCH

        PRINT '================================================================='

        SELECT
            @lv_Msg_Desc = 'END PROCESS   dbo.usp_cotiviti_daily_claim_cds_insertion_impl at ' + CONVERT(CHAR(27), GETDATE(), 109)

        PRINT @lv_Msg_Desc

        PRINT '================================================================='

        SELECT
            @lv_Msg_Desc = 'Return code from  dbo.usp_cotiviti_daily_claim_cds_insertion_impl :  ' + CAST(@lv_return_code AS VARCHAR)

        PRINT @lv_Msg_Desc

    END