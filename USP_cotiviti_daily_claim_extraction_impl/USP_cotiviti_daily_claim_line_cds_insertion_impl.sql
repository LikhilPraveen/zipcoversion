USE [INTEGRATION_PLUS_DB]
GO
/****** Object:  StoredProcedure [dbo].[USP_cotiviti_daily_claim_line_cds_insertion_impl]    Script Date: 7/28/2023 3:53:10 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =========================================================================
  -- Author          : UST OFFSHORE
  -- Create date     : 04/11/2022
  -- Description     : To retrive the claim line level details
  -- Tables involved :
                      --                   ALL_CLAIM_FACT
                      --                   ALL_CLAIM_LINE_FACT
                      --                   ALL_ACCOUNT_HISTORY_FACT
                      --                   ALL_MEMBER_VERSION_HIST_FACT
                      --                   ALL_PRACTITIONER_HISTORY_FACT
                      --                   ALL_SUPPLIER_HISTORY_FACT
                      --                   CLAIM_FACT_TO_DIAGNOSIS
                      --                   DIAGNOSIS
                       --                  CLAIM_LINE_FACT_TO_DIAG
                      --                   PAYMENT_FACT
                      --                   PAYMENT_FACT_TO_CLAIM_FACT
                      --                   POSTAL_ADDRESS
                      --                   PROVIDER_TAXONOMY
                      --                   TAX_ENTITY_HISTORY_FACT
                      --                   MEMBER_HIST_FACT_TO_BNFT_PLAN
                      --                   PRODUCT_TO_LINE_OF_BUSINESS
                      --                   LINE_OF_BUSINESS
                      --                   ADJUDICATION_DETAILS
                      --                   CLAIM_LINE_FCT_TO_COB_PAYMENT
                      --                   CLAIM_LN_FACT_TO_NDC_CODE_INFO
                      --                   NDC_CODE_INFO_FACT


  --=============================================================================
  --Version History		--Date             Changed by          		Description
  --V1.0					04/05/2022       UST OFFSHORE           Initial creation
  --V1.1					17/10/2022	     UST Offshore       	Modified to include cotiviti 2.3 layout changes
  --V1.2					18/07/2023		 UST Offshore			Modified Insurance LOB to pick from HRP (INTP-4512)
  --V1.3					21/07/2023		 UST Offshore			New Mapping Created and Corrected Map values (INTP-4769)
  --V1.4				    24/07/2023		 UST Offshore			Modified Medicaid to Medicare
  --V1.5					25/07/2023		 UST Offshore			Modified COB to get default 0$ values
  --V1.6					26/07/2023		 UST Offshore			Modified SUB_UNITS (CL_FT.ORIGINAL_LINE_NUMBER TO CLAIM_LINE_HCC_ID)
  CREATE OR ALTER PROCEDURE [dbo].[USP_cotiviti_daily_claim_line_cds_insertion_impl]
  (
    @pCLAIM_FACT_KEY NUMERIC(19) = NULL,
    @pCLAIM_HCC_ID   VARCHAR(30) = NULL
  )
  AS

  BEGIN

  	SET ANSI_NULLS ON
  	SET QUOTED_IDENTIFIER ON
  	SET NOCOUNT ON
	SET ANSI_WARNINGS OFF

 BEGIN TRY

    /*VARIABLE DECLARATION AND INITIALIZATION */
    DECLARE @li_return_code            	  	INT,
			@lv_Msg_Desc               	  	VARCHAR(900),
			@ldt_Log_DTM               	  	DATETIME,
			@lv_Msg_Type               	  	VARCHAR(50),
			@ln_claim_fact_key             	NUMERIC(19 ) = NULL,
            @ln_referring_practitioner_key  NUMERIC(19 ) = NULL,
			@ln_supplier_key                NUMERIC(19)  = NULL,
			--@ln_member_key                  NUMERIC(19)  = NULL,----moved to claim level in 2.3
			--@ln_account_key                 NUMERIC(19)  = NULL,--moved to claim level in 2.3
			--@lv_group_name                  VARCHAR(50 ) = '',--moved to claim level in 2.3
			@lv_claim_type                  VARCHAR(50 ) = '',
			@ln_statement_start_date_key    NUMERIC(19)  = NULL,
			@lv_claim_hcc_id                VARCHAR(25)  = NULL

    SELECT @ln_claim_fact_key = @pCLAIM_FACT_KEY

    /*TEMP TABLE FOR STORING CLAIM  LEVEL DETAILS */
    DROP TABLE IF EXISTS  #ALL_CLAIM_FACT_TEMP
    CREATE TABLE  #ALL_CLAIM_FACT_TEMP
    (
      CLAIM_FACT_KEY                  NUMERIC(19)   NOT NULL,
      CLAIM_HCC_ID                    VARCHAR(25)   NOT NULL,
      SUPPLIER_KEY                    NUMERIC(19)   NULL,
      MEMBER_KEY                      NUMERIC(19)   NULL,
      SUPPLIER_LOCATION_KEY           NUMERIC(19)   NULL,
      REFERRING_PRACTITIONER_KEY      NUMERIC(19)   NULL,
      ATTENDING_PRACTITIONER_KEY      NUMERIC(19)   NULL,
      CLAIM_TYPE_NAME                 VARCHAR(50)   DEFAULT '',
      STATEMENT_START_DATE_KEY        NUMERIC(19)   NULL,
	  IS_VOIDED						  VARCHAR(3)	NULL,
	  IS_ADJUSTED					  VARCHAR(3)	NULL
    )

    INSERT INTO #ALL_CLAIM_FACT_TEMP
    SELECT
      @ln_claim_fact_key AS CLAIM_FACT_KEY,
      CLF.CLAIM_HCC_ID AS CLAIM_HCC_ID,
      CLF.SUPPLIER_KEY,
      CLF.MEMBER_KEY,
      CLF.LOCATION_KEY,
      CLF.REFERRING_PRACTITIONER_KEY,
      CLF.ATTENDING_PRACTITIONER_KEY,
      CLF.CLAIM_TYPE_NAME,
      CLF.STATEMENT_START_DATE_KEY,
	  CLF.IS_VOIDED,
	  CLF.IS_ADJUSTED
    FROM HRDW_REPLICA.PAYOR_DW.ALL_CLAIM_FACT CLF
    WHERE CLF.CLAIM_FACT_KEY = @ln_claim_fact_key
		AND ISNULL(UPPER(CLF.DELETED_FLAG), '') <> 'Y'

    /* TO GET INITIAL VALUES FOR VARIABLE */
    SELECT
      @ln_referring_practitioner_key =  CL_TEMP.REFERRING_PRACTITIONER_KEY,
      @lv_claim_type = CL_TEMP.CLAIM_TYPE_NAME,
      @ln_statement_start_date_key = CL_TEMP.STATEMENT_START_DATE_KEY,
      @ln_supplier_key  = CL_TEMP.SUPPLIER_KEY,
      --@ln_member_key = CL_TEMP.MEMBER_KEY,--2.3 changes
      @lv_claim_hcc_id = CL_TEMP.CLAIM_HCC_ID
    FROM #ALL_CLAIM_FACT_TEMP CL_TEMP

    /*TEMP TABLE FOR STORING CLAIM LINE LEVEL DETAILS */
    DROP TABLE IF EXISTS  #ALL_CLAIM_LINE_FACT_TEMP
    CREATE TABLE  #ALL_CLAIM_LINE_FACT_TEMP
    (
      CLAIM_FACT_KEY            NUMERIC(19),
      CLAIM_LINE_FACT_KEY       NUMERIC(19),
      CLAIM_LINE_HCC_ID         VARCHAR(50)	  NULL,
      REVENUE_CODE              VARCHAR(50 )  DEFAULT '',
      UNIT_COUNT                NUMERIC(19,5) NULL,
      BILLED_AMOUNT             NUMERIC(19,2) NULL,
	  BASE_NON_COVERED_AMOUNT	NUMERIC(19,2) NULL, --2.3 changes
      BASE_ALLOWED_AMOUNT       NUMERIC(19,2) NULL,
      CLAIM_LINE_STATUS_CODE    VARCHAR(2)    DEFAULT '',
      BASE_COPAY_AMOUNT         NUMERIC(19,2) NULL,
      BASE_COINSURANCE_AMOUNT   NUMERIC(19,2) NULL,
      BASE_DEDUCTIBLE_AMOUNT    NUMERIC(19,2) NULL,
      PAID_AMOUNT               NUMERIC(19,2) NULL,
      PLACE_OF_SERVICE_CODE     VARCHAR (50 ) DEFAULT '',
      SERVICE_CODE              VARCHAR (50 ) NULL,
      SERVICE_START_DATE_KEY    NUMERIC(19)   NULL,
      SERVICE_END_DATE_KEY      NUMERIC(19)   NULL,
      ADJUDICATION_DETAILS_KEY  NUMERIC(19)   NULL,
      ACCOUNT_KEY               NUMERIC(19)   NULL,
      PRACTITIONER_KEY          NUMERIC(19)   NULL,
      SUPPLIER_KEY              NUMERIC(19)   NULL,
      MEMBER_KEY                NUMERIC(19)   NULL,
      SUPPLIER_LOCATION_KEY     NUMERIC(19)   NULL,
      OTHER_DISCOUNT_AMOUNT     NUMERIC(19,2) NULL,
      ORIGINAL_LINE_NUMBER      VARCHAR(50)   DEFAULT '',
      REFERRING_PRACTITIONER_KEY  NUMERIC(19) NULL,
	  MANUAL_ALLOWED_AMOUNT			NUMERIC(19,2) NULL,
	  MANUAL_BENEFIT_NETWORK_KEY	NUMERIC(19) NULL,
	  MANUAL_REPRICER_KEY			NUMERIC(19) NULL,
	  IS_SPLIT					VARCHAR(3)	NULL,
	  IS_VOIDED					VARCHAR(3)	NULL,
	  IS_ADJUSTED				VARCHAR(3)	NULL,
	  LINE_CAPITATION_INDICATOR VARCHAR (1) NULL --2.3 changes
    )

	/* TO GET CLAIM LINE LEVEL DETAILS */
	--DEBUG-- PRINT ' TO GET CLAIM LINE LEVEL DETAILS Started : ' + CONVERT( varchar, Getdate(),121)

    INSERT INTO  #ALL_CLAIM_LINE_FACT_TEMP
    SELECT
        @ln_claim_fact_key AS CLAIM_FACT_KEY,
        CL_FT.CLAIM_LINE_FACT_KEY,
        LEFT(CL_FT.CLAIM_LINE_HCC_ID,4) ,--2.3 changes
        LEFT(CL_FT.REVENUE_CODE,4) ,
        CL_FT.UNIT_COUNT ,
        CL_FT.BILLED_AMOUNT ,
		CL_FT.BASE_NON_COVERED_AMOUNT,--2.3 changes
        CL_FT.BASE_ALLOWED_AMOUNT ,
        CL_FT.CLAIM_LINE_STATUS_CODE ,
        CL_FT.BASE_COPAY_AMOUNT ,
        CL_FT.BASE_COINSURANCE_AMOUNT ,
        CL_FT.BASE_DEDUCTIBLE_AMOUNT ,
        CL_FT.PAID_AMOUNT ,
        LEFT(CL_FT.PLACE_OF_SERVICE_CODE,2) ,
        LEFT(CL_FT.SERVICE_CODE,5),
        CL_FT.SERVICE_START_DATE_KEY,
        CL_FT.SERVICE_END_DATE_KEY,
        CL_FT.ADJUDICATION_DETAILS_KEY,
        CL_FT.ACCOUNT_KEY,
        CL_FT.PRACTITIONER_KEY,
        CLF.SUPPLIER_KEY,
        CLF.MEMBER_KEY,
        CLF.SUPPLIER_LOCATION_KEY,
        CL_FT.OTHER_DISCOUNT_AMOUNT,
        CL_FT.ORIGINAL_LINE_NUMBER,
        @ln_referring_practitioner_key AS REFERRING_PRACTITIONER_KEY,
		CL_FT.MANUAL_ALLOWED_AMOUNT,
		CL_FT.MANUAL_BENEFIT_NETWORK_KEY,
		CL_FT.MANUAL_REPRICER_KEY,
		CL_FT.IS_SPLIT,
		IS_VOIDED,
		IS_ADJUSTED,
		CL_FT.IS_CAPITATED --2.3 changes
    FROM  #ALL_CLAIM_FACT_TEMP CLF
          INNER JOIN
          HRDW_REPLICA.PAYOR_DW.ALL_CLAIM_LINE_FACT CL_FT  ON
          CLF.CLAIM_FACT_KEY = CL_FT.CLAIM_FACT_KEY
	WHERE  ISNULL(UPPER(CL_FT.DELETED_FLAG), '') <> 'Y'

	--DEBUG-- PRINT ' TO GET CLAIM LINE LEVEL DETAILS Completed : ' + CONVERT( varchar, Getdate(),121)
  	/* TO GET INITIAL VALUES FOR VARIABLE */
	--moved to claim level in 2.3 start
      /*SELECT
			@ln_account_key = MEM_HIST.ACCOUNT_KEY
      FROM HRDW_REPLICA.PAYOR_DW.ALL_MEMBER_VERSION_HIST_FACT MEM_HIST
      WHERE  MEM_HIST.MEMBER_KEY = @ln_member_key
		  AND  MEM_HIST.VERSION_EFF_DATE_KEY <= @ln_statement_start_date_key
		  AND  MEM_HIST.VERSION_EXP_DATE_KEY > @ln_statement_start_date_key
		  AND MEM_HIST.ENDOR_EXP_DATE > GETDATE()
		  AND UPPER(MEM_HIST.MEMBER_STATUS) NOT IN ('R','V')
		  AND ISNULL(UPPER(MEM_HIST.DELETED_FLAG), '') <> 'Y'  */
    --moved to claim level in 2.3 end
      DROP TABLE IF EXISTS  #PRACTITIONER_TEMP
      CREATE TABLE  #PRACTITIONER_TEMP
      (
        PRACTITIONER_KEY              NUMERIC(19)   NULL,
        CLAIM_LINE_FACT_KEY           NUMERIC(19)   NULL,
        PRACTITIONER_HCC_ID           VARCHAR(50)   NULL,
        TAX_ID                        VARCHAR(9 )   NULL,
        PRACTITIONER_NPI              VARCHAR(150)  NULL,
        PRACTITIONER_LAST_NAME        VARCHAR(107)  NULL,
        PRACTITIONER_FIRST_NAME       VARCHAR(107)  NULL,
        PRACTITIONER_MIDDLE_NAME      VARCHAR(107)  NULL,
        PRACTITIONER_NAME_SUFFIX      VARCHAR(107)  NULL,
        PROVIDER_TAXONOMY_CODE        VARCHAR(50)   NULL,
        ADDRESS_LINE                  VARCHAR(900)  NULL,
        ADDRESS_LINE_2                VARCHAR(900)  NULL,
        CITY_NAME                     VARCHAR(50)   NULL,
        STATE_CODE                    VARCHAR(900)  NULL,
        ZIP_CODE                      VARCHAR(5 )  NULL,
        ZIP_4_CODE                    VARCHAR(4 )   NULL,
        COUNTRY_CODE                  VARCHAR(3 )   NULL
      )


    --DEBUG-- PRINT ' TO GET PRACTITIONER AND PRACTITIONER ADDRESS DETAILS IN CLAIM LINE LEVEL Started : ' + CONVERT( varchar, Getdate(),121)
  	/* TO GET PRACTITIONER AND PRACTITIONER ADDRESS DETAILS */
    IF EXISTS (SELECT 1 FROM #ALL_CLAIM_LINE_FACT_TEMP
               WHERE  PRACTITIONER_KEY IS NOT  NULL )  AND @lv_claim_type = 'Professional'
    BEGIN

       INSERT INTO #PRACTITIONER_TEMP  /* FETCHING PRACTITIONER AND PRACTITIONER ADDRESS DETAILS */
       SELECT
         PR_HIST.PRACTITIONER_KEY AS PRACTITIONER_KEY,
         CL_TEMP.CLAIM_LINE_FACT_KEY,
         LEFT(PR_HIST.PRACTITIONER_HCC_ID,25),
         REPLACE(PR_HIST.TAX_ID,'-','') AS TAX_ID,
         LEFT(PR_HIST.PRACTITIONER_NPI,10),
         LEFT(PR_HIST.PRACTITIONER_LAST_NAME,60),
         LEFT(PR_HIST.PRACTITIONER_FIRST_NAME,35),
         LEFT(PR_HIST.PRACTITIONER_MIDDLE_NAME,25),
         LEFT(PR_HIST.PRACTITIONER_NAME_SUFFIX,10),
         LEFT(PRV_TX.PROVIDER_TAXONOMY_CODE,10),
         LEFT(PA.ADDRESS_LINE,55) ,
         LEFT(PA.ADDRESS_LINE_2,55),
         LEFT(PA.CITY_NAME,30),
         LEFT(PA.STATE_CODE,2),
         LEFT(PA.ZIP_CODE,5) AS ZIP_CODE,
         PA.ZIP_4_CODE,
         PA.COUNTRY_CODE
       FROM HRDW_REPLICA.PAYOR_DW.ALL_PRACTITIONER_HISTORY_FACT PR_HIST
             INNER JOIN
            #ALL_CLAIM_LINE_FACT_TEMP CL_TEMP ON
              PR_HIST.PRACTITIONER_KEY = CL_TEMP.PRACTITIONER_KEY
              LEFT JOIN
            HRDW_REPLICA.PAYOR_DW.POSTAL_ADDRESS PA ON
             PA.POSTAL_ADDRESS_KEY = PR_HIST.PRACTITIONER_CORR_ADDR_KEY
			 AND ISNULL(UPPER(PA.DELETED_FLAG), '') <> 'Y'
             LEFT JOIN
            HRDW_REPLICA.PAYOR_DW.PROVIDER_TAXONOMY PRV_TX ON
             PRV_TX.PROVIDER_TAXONOMY_KEY = PR_HIST.PRIMARY_SPECIALTY_KEY
			 AND ISNULL(UPPER(PRV_TX.DELETED_FLAG ), '') <> 'Y'
       WHERE
         PR_HIST.VERSION_EFF_DATE_KEY <= @ln_statement_start_date_key
         AND PR_HIST.VERSION_EXP_DATE_KEY >  @ln_statement_start_date_key
         AND UPPER(PR_HIST.PRACTITIONER_STATUS) NOT IN ('R','V')
		 AND ISNULL(UPPER(PR_HIST.DELETED_FLAG ), '') <> 'Y'
    END
    --DEBUG-- PRINT ' TO GET PRACTITIONER AND PRACTITIONER ADDRESS DETAILS Completed : ' + CONVERT( varchar, Getdate(),121)

        DROP TABLE IF EXISTS  #SUPPLIER_TEMP
        CREATE TABLE  #SUPPLIER_TEMP
         (
          SUPPLIER_KEY                  NUMERIC(19)   NULL,
          SUPPLIER_HCC_ID               VARCHAR(50)   NULL,
          SUPPLIER_NPI                  VARCHAR(50)   NULL,
          SUPPLIER_NAME                 VARCHAR(150)  NULL,
          ADDRESS_LINE                  VARCHAR(900 ) NULL,
          ADDRESS_LINE_2                VARCHAR(900 ) NULL,
          CITY_NAME                     VARCHAR (50 ) NULL,
          STATE_CODE                    VARCHAR (900) NULL,
          ZIP_CODE                      VARCHAR(5)   NULL,
          ZIP_4_CODE                    VARCHAR(4)    NULL,
          COUNTRY_CODE                  VARCHAR(3)    NULL,
          TAX_ID                        VARCHAR(9)   NULL,
          PROVIDER_TAXONOMY_CODE        VARCHAR(50)   NULL,
          IS_PCP                        VARCHAR(255)  NULL
        )
    --DEBUG-- PRINT ' TO GET SUPPLIER AND SUPPLIER ADDRESS DETAILS Started : ' + CONVERT( varchar, Getdate(),121)
      IF @ln_supplier_key IS NOT NULL AND @lv_claim_type = 'Professional'
      BEGIN

        INSERT INTO #SUPPLIER_TEMP  /* FETCHING SUPPLIER AND SUPPLIER ADDRESS DETAILS */
        SELECT
          @ln_supplier_key AS SUPPLIER_KEY,
          LEFT(SUP_HIST.SUPPLIER_HCC_ID,25),
          LEFT(SUP_HIST.SUPPLIER_NPI,10),
          LEFT(SUP_HIST.SUPPLIER_NAME,60),
          LEFT(PA.ADDRESS_LINE,55),
          LEFT(PA.ADDRESS_LINE_2,55),
          LEFT(PA.CITY_NAME,30),
          LEFT(PA.STATE_CODE,2),
          LEFT(PA.ZIP_CODE,5) AS ZIP_CODE,
          PA.ZIP_4_CODE,
          PA.COUNTRY_CODE,
          REPLACE(TAX_HST.TAX_ID,'-','') AS  TAX_ID,
          LEFT(PRV_TX.PROVIDER_TAXONOMY_CODE,10),
          SUP_HIST.IS_PCP
        FROM HRDW_REPLICA.PAYOR_DW.ALL_SUPPLIER_HISTORY_FACT SUP_HIST
               LEFT JOIN
             HRDW_REPLICA.PAYOR_DW.POSTAL_ADDRESS PA ON
               PA.POSTAL_ADDRESS_KEY = SUP_HIST.SUPPLIER_CORR_ADDRESS_KEY
			   AND ISNULL(UPPER(PA.DELETED_FLAG), '') <> 'Y'
               LEFT JOIN
             HRDW_REPLICA.PAYOR_DW.PROVIDER_TAXONOMY PRV_TX ON
               PRV_TX.PROVIDER_TAXONOMY_KEY = SUP_HIST.PRIMARY_CLASSIFICATION_KEY
			   AND ISNULL(UPPER(PRV_TX.DELETED_FLAG), '') <> 'Y'
               LEFT JOIN
             HRDW_REPLICA.PAYOR_DW.TAX_ENTITY_HISTORY_FACT TAX_HST ON
               TAX_HST.TAX_ENTITY_KEY = SUP_HIST.TAX_ENTITY_KEY
			   AND TAX_HST.VERSION_EFF_DATE_KEY <= @ln_statement_start_date_key
			   AND TAX_HST.VERSION_EXP_DATE_KEY  > @ln_statement_start_date_key
			   AND ISNULL(UPPER(TAX_HST.DELETED_FLAG), '') <> 'Y'
        WHERE SUP_HIST.SUPPLIER_KEY = @ln_supplier_key
          AND  SUP_HIST.VERSION_EFF_DATE_KEY <= @ln_statement_start_date_key
          AND  SUP_HIST.VERSION_EXP_DATE_KEY  > @ln_statement_start_date_key
          AND UPPER(SUP_HIST.SUPPLIER_STATUS) NOT IN ('R','V')
		  AND ISNULL(UPPER(SUP_HIST.DELETED_FLAG), '') <> 'Y'
      END

	  --moved to claim level in 2.3

    /* TO GET GROUP_NAME */
   --DEBUG-- PRINT ' TO GET GROUP_NAME Started : ' + CONVERT( varchar, Getdate(),121)
   /* IF @ln_account_key IS NOT NULL AND @lv_claim_type = 'Professional'
    BEGIN
     SET @lv_group_name =----GROUP_NAME
       (
           SELECT ACC_HIST.ACCOUNT_HCC_ID AS GROUP_NAME
           FROM HRDW_REPLICA.PAYOR_DW.ALL_ACCOUNT_HISTORY_FACT ACC_HIST
            WHERE  ACC_HIST.ACCOUNT_KEY = @ln_account_key
			   AND  ACC_HIST.VERSION_EFF_DATE_KEY <= @ln_statement_start_date_key
			   AND  ACC_HIST.VERSION_EXP_DATE_KEY > @ln_statement_start_date_key
			   AND UPPER(ACC_HIST.ACCOUNT_STATUS) NOT IN ('R','V')
			   AND ISNULL(UPPER(ACC_HIST.DELETED_FLAG), '') <> 'Y'
        )
      END */
    --DEBUG-- PRINT ' TO GET GROUP_NAME Completed : ' + CONVERT( varchar, Getdate(),121)
    --moved to claim level in 2.3

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

    IF @ln_referring_practitioner_key IS NOT NULL AND @lv_claim_type = 'Professional'
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
         AND  PR_HIST.VERSION_EFF_DATE_KEY <= @ln_statement_start_date_key
         AND  PR_HIST.VERSION_EXP_DATE_KEY >  @ln_statement_start_date_key
         AND  UPPER(PR_HIST.PRACTITIONER_STATUS) NOT IN ('R','V')
		 AND  ISNULL(UPPER(PR_HIST.DELETED_FLAG), '') <> 'Y'
    END


      DROP TABLE IF EXISTS  #ALL_CLAIM_LINE_FACT_OTHER
      CREATE TABLE  #ALL_CLAIM_LINE_FACT_OTHER
      (
        CLAIM_FACT_KEY          NUMERIC(19),
        CLAIM_LINE_FACT_KEY     NUMERIC(19),
        DOS_FROM                DATE NULL,
        COB                     NUMERIC(19,2) NULL,
        DOS_TO                  DATE NULL,
        MODIFIER_CODE           VARCHAR(MAX )  DEFAULT '',
        ALLOWED_NDC             VARCHAR(50 ) DEFAULT '',
        ALLOWED_NDC_UNITS       NUMERIC(19,5) NULL,
        ALLOWED_NDC_UNITS_TYPE  VARCHAR(2 )  DEFAULT '',
        IS_NON_PAR              VARCHAR(1)    DEFAULT '',
        --GROUP_NAME              VARCHAR(50 )  DEFAULT '', --moved to claim level in 2.3
        DIAG_PTR                VARCHAR(MAX )  DEFAULT '',
        UNIT_COUNT              NUMERIC(19,5) NULL
      )

      /* TO GET OTHER COLUMNS RELATED  WITH CLAIM AND CLAIM LINE LEVEL */

    --DEBUG-- PRINT ' TO GET OTHER COLUMNS RELATED  WITH CLAIM AND CLAIM LEVEL Started : ' + CONVERT( varchar, Getdate(),121)

        /*--FETCHING OTHER COLUMNS RELATED  WITH #ALL_CLAIM_LINE_FACT --*/
        INSERT INTO #ALL_CLAIM_LINE_FACT_OTHER
        SELECT
          @ln_claim_fact_key AS CLAIM_FACT_KEY,
          CL_FT.CLAIM_LINE_FACT_KEY,
          DOS_FROM.DATE_VALUE AS DOS_FROM,
          CO_B.COB,
          DOS_TO.DATE_VALUE AS DOS_TO,
          MODI.SUB_MOD,
          LEFT(NDC.ALLOWED_NDC,11),
          NDC.ALLOWED_NDC_UNITS,
          NDC.ALLOWED_NDC_UNITS_TYPE,
          ADJU_DE.IS_NON_PAR,
         -- @lv_group_name AS GROUP_NAME, --moved to claim level in 2.3
          SIDE.DIAG_PTR,
          SUB_UNT.UNIT_COUNT
        FROM #ALL_CLAIM_LINE_FACT_TEMP  CL_FT
                LEFT JOIN  ----DOS_FROM
              HRDW_REPLICA.PAYOR_DW.DATE_DIMENSION DOS_FROM ON
                CL_FT.SERVICE_START_DATE_KEY = DOS_FROM.DATE_KEY
				AND ISNULL(UPPER(DOS_FROM.DELETED_FLAG), '') <> 'Y'
                 LEFT JOIN   ----DOS_TO
              HRDW_REPLICA.PAYOR_DW.DATE_DIMENSION DOS_TO ON
                CL_FT.SERVICE_END_DATE_KEY = DOS_TO.DATE_KEY
				AND ISNULL(UPPER(DOS_TO.DELETED_FLAG), '') <> 'Y'
                LEFT JOIN    ----PAR_YN
              HRDW_REPLICA.PAYOR_DW.ADJUDICATION_DETAILS ADJU_DE ON
                CL_FT.ADJUDICATION_DETAILS_KEY = ADJU_DE.ADJUDICATION_DETAILS_KEY
				AND ISNULL(UPPER(ADJU_DE.DELETED_FLAG), '') <> 'Y'
                OUTER APPLY ----COB
                (
                    SELECT
                      SUM(COB_FT.PAID_AMOUNT) AS COB
                    FROM HRDW_REPLICA.PAYOR_DW.CLAIM_LINE_FCT_TO_COB_PAYMENT CL_CP
                      INNER JOIN
                    HRDW_REPLICA.PAYOR_DW.COB_PAYMENT_FACT COB_FT ON
                      CL_CP.COB_PAYMENT_FACT_KEY = COB_FT.COB_PAYMENT_FACT_KEY
                    WHERE CL_FT.CLAIM_LINE_FACT_KEY = CL_CP.CLAIM_LINE_FACT_KEY
					  AND ISNULL(UPPER(CL_CP.DELETED_FLAG), '') <> 'Y'
					  AND ISNULL(UPPER(COB_FT.DELETED_FLAG), '') <> 'Y'
                )CO_B
                OUTER APPLY ----NDC
                (
                    SELECT
                      NDC_CODE AS ALLOWED_NDC,
                      QUANTITY AS ALLOWED_NDC_UNITS,
                      MEASUREMENT_TYPE_CODE AS ALLOWED_NDC_UNITS_TYPE
                    FROM HRDW_REPLICA.PAYOR_DW.CLAIM_LN_FACT_TO_NDC_CODE_INFO CL_CI
                        INNER JOIN
                    HRDW_REPLICA.PAYOR_DW.NDC_CODE_INFO_FACT   ND_FT ON
                        CL_CI.NDC_CODE_INFO_KEY = ND_FT.NDC_CODE_INFO_KEY
                    WHERE CL_FT.CLAIM_LINE_FACT_KEY = CL_CI.CLAIM_LINE_FACT_KEY
					  AND ISNULL(UPPER(CL_CI.DELETED_FLAG), '') <> 'Y'
					  AND ISNULL(UPPER(ND_FT.DELETED_FLAG), '') <> 'Y'
                )NDC
                OUTER APPLY  ---CLAIM_LINE_DIAGNOSIS POINTER
                (
                  SELECT
                	  CFTD.SORT_ORDER  AS CLM_LN_DIAG_POINTER,
                     ROW_NUMBER() OVER (PARTITION BY CL_FT.CLAIM_FACT_KEY, CL_FT.CLAIM_LINE_FACT_KEY
                     ORDER BY CL_FT.CLAIM_LINE_HCC_ID) AS SORT_ORDER
                  FROM HRDW_REPLICA.PAYOR_DW.CLAIM_LINE_FACT_TO_DIAG CLFTD
                		  INNER JOIN
                       HRDW_REPLICA.PAYOR_DW.CLAIM_FACT_TO_DIAGNOSIS CFTD ON
                          CFTD.DIAGNOSIS_CODE = CLFTD.DIAGNOSIS_CODE
                  WHERE CL_FT.CLAIM_FACT_KEY = CFTD.CLAIM_FACT_KEY
                      AND CL_FT.CLAIM_LINE_FACT_KEY = CLFTD.CLAIM_LINE_FACT_KEY
					  AND ISNULL(UPPER(CLFTD.DELETED_FLAG), '') <> 'Y'
					  AND ISNULL(UPPER(CFTD.DELETED_FLAG), '') <> 'Y'
                      FOR XML PATH  ('DIAG_PTR'), ROOT ('DIAG_PTR_LIST')
                )AS SIDE(DIAG_PTR)
                OUTER APPLY  -------MODIFIER
                (
                  SELECT
                	LEFT(MOD.MODIFIER_CODE,2)  AS SUB_MOD,
                    MOD.SORT_ORDER AS SORT_ORDER
                  FROM HRDW_REPLICA.PAYOR_DW.CLAIM_LINE_FACT_TO_MODIFIER   MOD
                  WHERE  CL_FT.CLAIM_LINE_FACT_KEY = MOD.CLAIM_LINE_FACT_KEY
					AND ISNULL(UPPER(MOD.DELETED_FLAG), '') <> 'Y'
                  ORDER BY MOD.SORT_ORDER
                  FOR XML PATH  ('SUB_MOD'), ROOT ('SUB_MOD_LIST')
                )AS MODI(SUB_MOD)
                 OUTER APPLY  ---SUB_UNITS
                (
                  SELECT
                		TOP 1		SUB_UN.UNIT_COUNT
                	FROM #ALL_CLAIM_LINE_FACT_TEMP    SUB_UN
                	WHERE  CL_FT.CLAIM_FACT_KEY = SUB_UN.CLAIM_FACT_KEY
                      --AND  CL_FT.ORIGINAL_LINE_NUMBER  = SUB_UN.CLAIM_LINE_HCC_ID --V1.6
						AND  CL_FT.CLAIM_LINE_HCC_ID  = SUB_UN.CLAIM_LINE_HCC_ID
                )AS SUB_UNT

		--2.3 changes start

	  DROP TABLE IF EXISTS #SUPPLIER_OTHER_ID

      CREATE TABLE #SUPPLIER_OTHER_ID
      (
        CLAIM_FACT_KEY            NUMERIC(19)
       ,SUPPLIER_HISTORY_FACT_KEY NUMERIC(19)
       ,MEDICARE_PROVIDER_ID      VARCHAR(80)
      )

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
      FROM #ALL_CLAIM_FACT_TEMP CLAIM
	  INNER JOIN HRDW_REPLICA.PAYOR_DW.ALL_SUPPLIER_HISTORY_FACT ASHF1
	  ON CLAIM.SUPPLIER_KEY = ASHF1.SUPPLIER_KEY
      INNER JOIN HRDW_REPLICA.PAYOR_DW.SUPPLIER_OTHER_ID SOI
      ON ASHF1.SUPPLIER_HISTORY_FACT_KEY = SOI.SUPPLIER_HISTORY_FACT_KEY
      INNER JOIN HRDW_REPLICA.PAYOR_DW.ID_TYPE_CODE ITC
      ON SOI.ID_TYPE_KEY = ITC.ID_TYPE_KEY
      WHERE
		  SOI.EFFECTIVE_START_DATE_KEY <=  @ln_statement_start_date_key --To get the id which is effective between the claim's date of service
	  AND SOI.EFFECTIVE_END_DATE_KEY   >   @ln_statement_start_date_key --To get the id which is effective between the claim's date of service
	  AND ISNULL(UPPER(SOI.DELETED_FLAG), '') <> 'Y'
	  AND ITC.ID_TYPE_CODE='1C' --To get the medicare provider id
	  AND ISNULL(UPPER(ITC.DELETED_FLAG), '') <> 'Y'
	  AND ASHF1.VERSION_EFF_DATE_KEY <=  @ln_statement_start_date_key
	  AND ASHF1.VERSION_EXP_DATE_KEY >   @ln_statement_start_date_key
	  AND UPPER(ASHF1.SUPPLIER_STATUS) NOT IN ('R','V') --Record should not be in Workbasket (Repair,Review)
	  AND ISNULL(UPPER(ASHF1.DELETED_FLAG), '') <> 'Y'
	  ) MEDPROVID
	  WHERE RN=1


	DROP TABLE IF EXISTS  #INSURANCE_LOB
    CREATE TABLE  #INSURANCE_LOB
    (
      CLAIM_FACT_KEY            NUMERIC(19),
      MEMBER_KEY                NUMERIC(19),
	  LINE_OF_BUSINESS_NAME		VARCHAR(15), --V1.2
	  LOB_PRODUCT_LINE			VARCHAR(15)  --V1.2
	 )

	INSERT INTO #INSURANCE_LOB
	SELECT
			CLAIM.CLAIM_FACT_KEY,
			CLAIM.MEMBER_KEY,
			LEFT(LOB.LINE_OF_BUSINESS_NAME, 15),
			LEFT(LOB.LOB_PRODUCT_LINE, 15)  --V1.2
	FROM #ALL_CLAIM_FACT_TEMP CLAIM
		INNER JOIN HRDW_REPLICA.PAYOR_DW.ALL_MEMBER_VERSION_HIST_FACT MEM_HIST
		ON MEM_HIST.MEMBER_KEY = CLAIM.MEMBER_KEY
		INNER JOIN HRDW_REPLICA.PAYOR_DW.MEMBER_HIST_FACT_TO_BNFT_PLAN MEM_BP
		ON MEM_HIST.MEMBER_HISTORY_FACT_KEY = MEM_BP.MEMBER_HISTORY_FACT_KEY
		INNER JOIN
			(SELECT PROD_TO_LOB.PRODUCT_KEY,
				MAX(CASE
				WHEN LOB.LOB_GROUP_NAME = 'Product Line' THEN LOB.LINE_OF_BUSINESS_NAME
				ELSE NULL
				END) AS LOB_PRODUCT_LINE,
				MAX(CASE
               WHEN LOB.LOB_GROUP_NAME = 'Line of Business' THEN LOB.LINE_OF_BUSINESS_NAME
               ELSE NULL
				END) AS LINE_OF_BUSINESS_NAME
			FROM HRDW_REPLICA.PAYOR_DW.PRODUCT_TO_LINE_OF_BUSINESS PROD_TO_LOB WITH(NOLOCK)
			LEFT JOIN HRDW_REPLICA.PAYOR_DW.LINE_OF_BUSINESS LOB WITH(NOLOCK) ON LOB.LINE_OF_BUSINESS_KEY = PROD_TO_LOB.LINE_OF_BUSINESS_KEY
			WHERE
				ISNULL(UPPER(PROD_TO_LOB.DELETED_FLAG), '') <> 'Y'
				AND ISNULL(UPPER(LOB.DELETED_FLAG), '') <> 'Y'
			GROUP BY PROD_TO_LOB.PRODUCT_KEY
			) LOB ON MEM_BP.PRODUCT_KEY = LOB.PRODUCT_KEY
	WHERE
		  MEM_HIST.VERSION_EFF_DATE_KEY <= @ln_statement_start_date_key
			AND  MEM_HIST.VERSION_EXP_DATE_KEY > @ln_statement_start_date_key
			AND  MEM_HIST.ENDOR_EXP_DATE > GETDATE()
			AND UPPER(MEM_HIST.MEMBER_STATUS) NOT IN ('R','V')
			AND ISNULL(UPPER(MEM_HIST.DELETED_FLAG), '') <> 'Y'
			AND ISNULL(UPPER(MEM_BP.DELETED_FLAG), '') <> 'Y'

	 --2.3 changes end

      --DEBUG-- PRINT ' TO GET OTHER COLUMNS RELATED  WITH CLAIM AND CLAIM LEVEL Completed : ' + CONVERT( varchar, Getdate(),121)

       /* TO GET FETCHING FINAL RESULT SET */
       --DEBUG-- PRINT ' TO GET FETCHING FINAL RESULT SET Started : ' + CONVERT( varchar, Getdate(),121)

          IF @lv_claim_type = 'Professional'
          BEGIN
                DROP TABLE IF EXISTS #ustil_hrp_stg_cotiviti_claimsLines_professional

				CREATE TABLE #ustil_hrp_stg_cotiviti_claimsLines_professional
                (
                    [CLAIM_LINE_FACT_KEY]            [NUMERIC] (19,0) NOT NULL,
                    [CLAIM_ID]                       [VARCHAR] (25) NOT NULL,
                    [CLAIM_FACT_KEY]                 [NUMERIC] (19,0) NOT NULL,
					[CLAIM_LINE_NUMBER_HRP]			 [VARCHAR] (50)  NULL,
					[CLAIM_LINE_NUMBER_COTIVITI]	 [VARCHAR] (50)  NULL,
                    [INSURANCE_ID]                   [VARCHAR] (15) NULL, --V1.2
                    [RENDERING_PROVIDER_ID]          [VARCHAR] (25) NULL,
                    [RENDERING_TAXGROUP_ID]          [VARCHAR] (9) NULL,
                    [RENDERING_NPI]                  [NUMERIC] (10) NULL,
                    [RENDERING_STATE_LICENSE]        [VARCHAR] (10) NULL,
                    [RENDERING_SUBSPEC_ID]           [VARCHAR] (10) NULL,
                    [RENDERING_ENTITY_TYPE]          [VARCHAR] (1) NULL,
                    [RENDERING_LAST_NAME]            [VARCHAR] (60) NULL,
                    [RENDERING_FIRST_NAME]           [VARCHAR] (35) NULL,
                    [RENDERING_MIDDLE_NAME]          [VARCHAR] (25) NULL,
                    [RENDERING_NAME_SUFFIX]          [VARCHAR] (10) NULL,
                    [RENDERING_STREET_ADDRESS_1]     [VARCHAR] (55) NULL,
                    [RENDERING_STREET_ADDRESS_2]     [VARCHAR] (55) NULL,
                    [RENDERING_CITY]                 [VARCHAR] (30) NULL,
                    [RENDERING_STATE]                [VARCHAR] (2) NULL,
                    [RENDERING_ZIP]                  [VARCHAR] (5) NULL,
                    [RENDERING_ZIP_PLUS_4]           [VARCHAR] (4) NULL,
                    [RENDERING_COUNTRY]              [VARCHAR] (3) NULL,
                    [RENDERING_COUNTRY_SUBDIVISION]  [VARCHAR] (3) NULL,
                    [RENDERING_PHONE_COUNTRY_CODE]   [VARCHAR] (5) NULL,
                    [RENDERING_PHONE]                [VARCHAR] (10) NULL,
                    [RENDERING_PHONE_EXT]            [VARCHAR] (6) NULL,
                    [RENDERING_ALT_PHONE_COUNTRY_CODE]  [VARCHAR] (5) NULL,
                    [RENDERING_ALT_PHONE]            [VARCHAR] (10) NULL,
                    [RENDERING_ALT_PHONE_EXT]        [VARCHAR] (6) NULL,
                    [RENDERING_FAX_COUNTRY_CODE]     [VARCHAR] (5) NULL,
                    [RENDERING_FAX]                  [VARCHAR] (10) NULL,
                    [BILLING_PROVIDER_ID]            [VARCHAR] (25) NULL,
                    [BILLING_TAXGROUP_ID]            [VARCHAR] (9) NULL,
                    [BILLING_NPI]                    [NUMERIC] (10) NULL,
                    [BILLING_STATE_LICENSE]          [VARCHAR] (10) NULL,
                    [BILLING_SUBSPEC_ID]             [VARCHAR] (10) NULL,
                    [BILLING_CURRENCY_CODE]          [VARCHAR] (3) NULL,
                    [BILLING_ENTITY_TYPE]            [VARCHAR] (1) NULL,
                    [BILLING_LAST_NAME]              [VARCHAR] (255) NULL,
                    [BILLING_FIRST_NAME]             [VARCHAR] (35) NULL,
                    [BILLING_MIDDLE_NAME]            [VARCHAR] (25) NULL,
                    [BILLING_NAME_SUFFIX]            [VARCHAR] (10) NULL,
                    [BILLING_ADDRESS_1]              [VARCHAR] (255) NULL,
                    [BILLING_ADDRESS_2]              [VARCHAR] (255) NULL,
                    [BILLING_CITY]                   [VARCHAR] (50) NULL,
                    [BILLING_STATE]                  [VARCHAR] (2) NULL,
                    [BILLING_ZIP]                    [VARCHAR] (5) NULL,
                    [BILLING_ZIP_PLUS_4]             [VARCHAR] (4) NULL,
                    [BILLING_COUNTRY]                [VARCHAR] (3) NULL,
                    [BILLING_COUNTRY_SUBDIVISION]    [VARCHAR] (3) NULL,
                    [BILLING_CONTACT]                [VARCHAR] (60) NULL,
                    [BILLING_PHONE_COUNTRY]          [VARCHAR] (5) NULL,
                    [BILLING_PHONE]                  [VARCHAR] (10) NULL,
                    [BILLING_PHONE_EXT]              [VARCHAR] (6) NULL,
                    [BILLING_ALT_PHONE_COUNTRY_CODE] [VARCHAR] (5) NULL,
                    [BILLING_ALT_PHONE]              [VARCHAR] (10) NULL,
                    [BILLING_ALT_PHONE_EXT]          [VARCHAR] (6) NULL,
                    [BILLING_FAX_COUNTRY_CODE]       [VARCHAR] (5) NULL,
                    [BILLING_FAX]                    [VARCHAR] (10) NULL,
                    [CONTRACT_ID]                    [VARCHAR] (25) NULL,
                    [CONTRACT_TYPE_CODE]             [VARCHAR] (2) NULL,
                    [CONTRACT_AMOUNT]                [NUMERIC] (19,2) NULL,
                    [CONTRACT_PCT]                   [NUMERIC] (10,5) NULL,
                    [CONTRACT_CODE]                  [VARCHAR] (50) NULL,
                    [CONTRACT_TERMS_DISC_PCT]        [NUMERIC] (10,5) NULL,
                    [CONTRACT_VERSION_ID]            [VARCHAR] (30) NULL,
                    [LINE_SEQ]                       [VARCHAR] (50) NULL,
                    [DOS_FROM]                        [DATE] NULL,
                    [DOS_TO]                          [DATE] NULL,
                    [POS_ID]                          [VARCHAR] (2) NULL,
                    [TOS]                             [VARCHAR] (1) NULL,
                    [DIAG_PTR_LIST]                   [VARCHAR] (MAX) NULL,
                    [SUB_HCPCS]                       [VARCHAR] (5) NULL,
                    [SUB_MOD_LIST]                    [VARCHAR] (MAX) NULL,
                    [UNITS_TYPE]                      [VARCHAR] (2) NULL,
                    [SUB_UNITS]                       [NUMERIC] (19,5) NULL,
                    [SUB_AMOUNT]                      [NUMERIC] (19,2) NULL,
                    [ALLOWED_HCPCS]                   [VARCHAR] (5) NULL,
                    [ALLOWED_MOD1]                    [VARCHAR] (2) NULL,
                    [ALLOWED_MOD2]                    [VARCHAR] (2) NULL,
                    [ALLOWED_MOD3]                    [VARCHAR] (2) NULL,
                    [ALLOWED_MOD4]                    [VARCHAR] (2) NULL,
                    [ALLOWED_MOD5]                    [VARCHAR] (2) NULL,
                    [ALLOWED_MOD6]                    [VARCHAR] (2) NULL,
                    [ALLOWED_MOD7]                    [VARCHAR] (2) NULL,
                    [ALLOWED_MOD8]                    [VARCHAR] (2) NULL,
                    [ALLOWED_UNITS]                   [NUMERIC] (19,5) NULL,
                    [ALLOWED_AMOUNT]                  [NUMERIC] (19,2) NULL,
                    [SUB_NDC]                         [VARCHAR] (11) NULL,
                    [SUB_NDC_UNITS]                   [NUMERIC] (19,5) NULL,
                    [SUB_NDC_UNITS_TYPE]              [VARCHAR] (2) NULL,
                    [COMPOUND_DRUG_YN]                [VARCHAR] (1) NULL,
                    [ALLOWED_NDC]                     [VARCHAR] (13) NULL,
                    [ALLOWED_NDC_UNITS]               [NUMERIC] (19,5) NULL,
                    [ALLOWED_NDC_UNITS_TYPE]          [VARCHAR] (2) NULL,
                    [COPAY]                           [NUMERIC] (19,2) NULL,
                    [COINSURANCE]                     [NUMERIC] (19,2) NULL,
                    [DEDUCTIBLE]                      [NUMERIC] (19,2) NULL,
                    [COB]                             [NUMERIC] (19,2) NULL,
                    [OTHER_REDUCTION]                 [NUMERIC] (19,2) NULL,
                    [PAID]                            [NUMERIC] (19,2) NULL,
                    [PAID_DATE]                       [DATE] NULL,
                    [BYPASS_CODE]                     [NUMERIC] (20) DEFAULT 0,
                    [PAR_YN]                          [VARCHAR] (1) NULL,
                    [EDIT_0_ALLOWED_YN]               [VARCHAR] (1) NULL,
                    [LINE_SEQ_ORIG]                   [VARCHAR] (50) NULL,
                    --[GROUP_NAME]                      [VARCHAR] (60) NULL,--this field is in claim level as per cotiviti 2.3 layout. so added in the header table
                    [PAY_TO_ENTITY_TYPE]                [VARCHAR] (1) NULL,
                    [PAY_TO_LAST_NAME]                  [VARCHAR] (60) NULL,
                    [PAY_TO_FIRST_NAME]                 [VARCHAR] (35) NULL,
                    [PAY_TO_MIDDLE_NAME]                [VARCHAR] (25) NULL,
                    [PAY_TO_NAME_SUFFIX]                [VARCHAR] (10) NULL,
                    [PAY_TO_ADDRESS_1]                  [VARCHAR] (55) NULL,
                    [PAY_TO_ADDRESS_2]                  [VARCHAR] (55) NULL,
                    [PAY_TO_CITY]                       [VARCHAR] (30) NULL,
                    [PAY_TO_STATE]                      [VARCHAR] (2) NULL,
                    [PAY_TO_ZIP]                        [VARCHAR] (5) NULL,
                    [PAY_TO_ZIP_PLUS_4]                 [VARCHAR] (4) NULL,
                    [PAY_TO_COUNTRY]                    [VARCHAR] (3) NULL,
                    [PAY_TO_COUNTRY_SUBDIVISION]        [VARCHAR] (3) NULL,
                    [REFERRING_PCP_YN]                  [VARCHAR] (1) NULL,
                    [REFERRING_PROVIDER_ID]             [VARCHAR] (25) NULL,
                    [REFERRING_TAXGROUP_ID]             [VARCHAR] (9) NULL,
                    [REFERRING_NPI]                     [NUMERIC] (10) NULL,--older [varchar](10) 2.3 changes [numeric](10, 0)
                    [REFERRING_STATE_LICENSE]           [VARCHAR] (10) NULL,
                    [REFERRING_SUBSPEC_ID]              [VARCHAR] (10) NULL,
                    [REFERRING_ENTITY_TYPE]             [VARCHAR] (1) NULL,
                    [REFERRING_LAST_NAME]               [VARCHAR] (60) NULL,
                    [REFERRING_FIRST_NAME]              [VARCHAR] (35) NULL,
                    [REFERRING_MIDDLE_NAME]             [VARCHAR] (25) NULL,
                    [REFERRING_NAME_SUFFIX]             [VARCHAR] (10) NULL,
                    [SUPERVISING_PROVIDER_ID]           [VARCHAR] (25) NULL,
                    [SUPERVISING_TAXGROUP_ID]           [VARCHAR] (9) NULL,
                    [SUPERVISING_NPI]                   [NUMERIC] (10) NULL,--older [VARCHAR] (10) 2.3 changes [numeric](10, 0)
                    [SUPERVISING_STATE_LICENSE]         [VARCHAR] (10) NULL,
                    [SUPERVISING_SUBSPEC_ID]            [VARCHAR] (10) NULL,
                    [SUPERVISING_ENTITY_TYPE]           [VARCHAR] (1) NULL,
                    [SUPERVISING_LAST_NAME]             [VARCHAR] (60) NULL,
                    [SUPERVISING_FIRST_NAME]            [VARCHAR] (35) NULL,
                    [SUPERVISING_MIDDLE_NAME]           [VARCHAR] (25) NULL,
                    [SUPERVISING_NAME_SUFFIX]           [VARCHAR] (10) NULL,
                    [ORDERING_PROVIDER_ID]              [VARCHAR] (25) NULL,
                    [ORDERING_TAXGROUP_ID]              [VARCHAR] (9) NULL,--older NUMERIC] (9) 2.3 changes [VARCHAR] (9)
                    [ORDERING_NPI]                      [NUMERIC] (10) NULL,
                    [ORDERING_STATE_LICENSE]            [VARCHAR] (10) NULL,
                    [ORDERING_SUBSPEC_ID]               [VARCHAR] (10) NULL,
                    [ORDERING_ENTITY_TYPE]              [VARCHAR] (1) NULL,
                    [ORDERING_LAST_NAME]                [VARCHAR] (60) NULL,
                    [ORDERING_FIRST_NAME]               [VARCHAR] (35) NULL,
                    [ORDERING_MIDDLE_NAME]              [VARCHAR] (25) NULL,
                    [ORDERING_NAME_SUFFIX]              [VARCHAR] (10) NULL,
                    [ORDERING_STREET_ADDRESS_1]         [VARCHAR] (55) NULL,
                    [ORDERING_STREET_ADDRESS_2]         [VARCHAR] (55) NULL,
                    [ORDERING_CITY]                     [VARCHAR] (30) NULL,
                    [ORDERING_STATE]                    [VARCHAR] (2) NULL,
                    [ORDERING_ZIP]                      [VARCHAR] (5) NULL,
                    [ORDERING_ZIP_PLUS_4]               [VARCHAR] (4) NULL,
                    [ORDERING_COUNTRY]                  [VARCHAR] (3) NULL,
                    [ORDERING_COUNTRY_SUBDIVISION]      [VARCHAR] (3) NULL,
                    [ORDERING_CONTACT]                  [VARCHAR] (60) NULL,
                    [ORDERING_PHONE_COUNTRY_CODE]       [VARCHAR] (5) NULL,
                    [ORDERING_PHONE]                    [VARCHAR] (10) NULL,
                    [ORDERING_PHONE_EXT]                [VARCHAR] (6) NULL,
                    [ORDERING_ALT_PHONE_COUNTRY_CODE]   [VARCHAR] (5) NULL,
                    [ORDERING_ALT_PHONE]                [VARCHAR] (10) NULL,
                    [ORDERING_ALT_PHONE_EXT]            [VARCHAR] (6) NULL,
                    [ORDERING_FAX_COUNTRY_CODE]         [VARCHAR] (5) NULL,
                    [ORDERING_FAX]                      [VARCHAR] (10) NULL,
                    [CHIRO_CONDITION_CODE]              [VARCHAR] (1) NULL,
                    [CHIRO_CONDITION_DESC_1]            [VARCHAR] (80) NULL,
                    [CHIRO_CONDITION_DESC_2]            [VARCHAR] (80) NULL,
                    [VISION_CODE_CATEGORY]              [VARCHAR] (2) NULL,
                    [VISION_CONDITION_CODE_1]           [VARCHAR] (3) NULL,
                    [VISION_CONDITION_CODE_2]           [VARCHAR] (3) NULL,
                    [VISION_CONDITION_CODE_3]           [VARCHAR] (3) NULL,
                    [VISION_CONDITION_CODE_4]           [VARCHAR] (3) NULL,
                    [VISION_CONDITION_CODE_5]           [VARCHAR] (3) NULL,
                    [EMERGENCY_INDICATOR_YN]            [VARCHAR] (1) NULL,
                    [FAMILY_PLANNING_INDICATOR_YN]      [VARCHAR] (1) NULL,
                    [DME_HCPCS]                         [VARCHAR] (5) NULL,
                    [DME_UNITS]                         [NUMERIC] (19,5) NULL,
                    [DME_RENTAL_PRICE]                  [NUMERIC] (19,2) NULL,
                    [DME_PURCHASE_PRICE]                [NUMERIC] (19,2) NULL,
                    [DME_RENTAL_FREQUENCY]              [NUMERIC] (1) NULL,
                    [DMERC_CMN_TRANSMIT_CODE]           [VARCHAR] (2) NULL,
                    [DME_CERT_TYPE]                     [VARCHAR] (1) NULL,
                    [DME_DURATION]                      [NUMERIC] (19,5) NULL,
                    [DME_CONDITION_CODE_1]              [VARCHAR] (3) NULL,
                    [DME_CONDITION_CODE_2]              [VARCHAR] (3) NULL,
                    [DME_CERT_REVISION_DATE]            [DATE]  NULL,
                    [DME_BEGIN_THERAPY_DATE]            [DATE] NULL,
                    [DME_LAST_CERT_DATE]                [DATE] NULL,
                    --[FORM_ID_CODE]                      [VARCHAR] (3) NULL,--this field is in claim level as per cotiviti 2.3 layout. so added in the header table
                    --[FORM_ID]                           [VARCHAR] (30) NULL,--this field is in claim level as per cotiviti 2.3 layout. so added in the header table
                    [LINE_ATTACHMENT_TYPE_CODE_1]       [VARCHAR] (2) NULL,
                    [LINE_ATTACHMENT_TRANSMIT_CODE_1]   [VARCHAR] (2) NULL,
                    [LINE_ACN_1]                        [VARCHAR] (24) NULL,
                    [LINE_ATTACHMENT_TYPE_CODE_2]       [VARCHAR] (2) NULL,
                    [LINE_ATTACHMENT_TRANSMIT_CODE_2]   [VARCHAR] (2) NULL,
                    [LINE_ACN_2]                        [VARCHAR] (24) NULL,
                    [LINE_ATTACHMENT_TYPE_CODE_3]       [VARCHAR] (2) NULL,
                    [LINE_ATTACHMENT_TRANSMIT_CODE_3]   [VARCHAR] (2) NULL,
                    [LINE_ACN_3]                        [VARCHAR] (24) NULL,
                    [LINE_ATTACHMENT_TYPE_CODE_4]       [VARCHAR] (2) NULL,
                    [LINE_ATTACHMENT_TRANSMIT_CODE_4]   [VARCHAR] (2) NULL,
                    [LINE_ACN_4]                        [VARCHAR] (24) NULL,
                    [LINE_ATTACHMENT_TYPE_CODE_5]       [VARCHAR] (2) NULL,
                    [LINE_ATTACHMENT_TRANSMIT_CODE_5]   [VARCHAR] (2) NULL,
                    [LINE_ACN_5]                        [VARCHAR] (24) NULL,
                    [LINE_ATTACHMENT_TYPE_CODE_6]       [VARCHAR] (2) NULL,
                    [LINE_ATTACHMENT_TRANSMIT_CODE_6]   [VARCHAR] (2) NULL,
                    [LINE_ACN_6]                        [VARCHAR] (24) NULL,
                    [LINE_ATTACHMENT_TYPE_CODE_7]       [VARCHAR] (2) NULL,
                    [LINE_ATTACHMENT_TRANSMIT_CODE_7]   [VARCHAR] (2) NULL,
                    [LINE_ACN_7]                        [VARCHAR] (24) NULL,
                    [LINE_ATTACHMENT_TYPE_CODE_8]       [VARCHAR] (2) NULL,
                    [LINE_ATTACHMENT_TRANSMIT_CODE_8]   [VARCHAR] (2) NULL,
                    [LINE_ACN_8]                        [VARCHAR] (24) NULL,
                    [LINE_ATTACHMENT_TYPE_CODE_9]       [VARCHAR] (2) NULL,
                    [LINE_ATTACHMENT_TRANSMIT_CODE_9]   [VARCHAR] (2) NULL,
                    [LINE_ACN_9]                        [VARCHAR] (24) NULL,
                    [LINE_ATTACHMENT_TYPE_CODE_10]      [VARCHAR] (2) NULL,
                    [LINE_ATTACHMENT_TRANSMIT_CODE_10]  [VARCHAR] (2) NULL,
                    [LINE_ACN_10]                       [VARCHAR] (24) NULL,
                    --[CHIRO_LAST_XRAY_DATE]              [DATE] NULL,--this field is in claim level as per cotiviti 2.3 layout. so added in the header table
                    [LINE_AUTH_NO_1]                    [VARCHAR] (30) NULL,
                    [LINE_AUTH_NO_1_NPI]                [NUMERIC] (10) NULL,
                    [LINE_AUTH_NO_2]                    [VARCHAR] (30) NULL,
                    [LINE_AUTH_NO_2_NPI]                [NUMERIC] (10) NULL,
                    [LINE_AUTH_NO_3]                    [VARCHAR] (30) NULL,
                    [LINE_AUTH_NO_3_NPI]                [NUMERIC] (10) NULL,
                    [LINE_AUTH_NO_4]                    [VARCHAR] (30) NULL,
                    [LINE_AUTH_NO_4_NPI]                [NUMERIC] (10) NULL,
                    [LINE_AUTH_NO_5]                    [VARCHAR] (30) NULL,
                    [LINE_AUTH_NO_5_NPI]                [NUMERIC] (10) NULL,
                    [LINE_NOTE_REF_CODE]                [VARCHAR] (3) NULL,
                    [LINE_NOTE_TEXT]                    [VARCHAR] (80) NULL,
                    [ADJ_CODE_1]                        [VARCHAR] (10) NULL,
                    [ADJ_TYPE_1]                        [VARCHAR] (2) NULL,
                    [ADJUSTOR_ID_1]                     [VARCHAR] (20) NULL,
                    [ADJ_CODE_2]                        [VARCHAR] (10) NULL,
                    [ADJ_TYPE_2]                        [VARCHAR] (2) NULL,
                    [ADJUSTOR_ID_2]                     [VARCHAR] (20) NULL,
                    [ADJ_CODE_3]                        [VARCHAR] (10) NULL,
                    [ADJ_TYPE_3]                        [VARCHAR] (2) NULL,
                    [ADJUSTOR_ID_3]                     [VARCHAR] (20) NULL,
                    [ADJ_CODE_4]                        [VARCHAR] (10) NULL,
                    [ADJ_TYPE_4]                        [VARCHAR] (2) NULL,
                    [ADJUSTOR_ID_4]                     [VARCHAR] (20) NULL,
                    [ADJ_CODE_5]                        [VARCHAR] (10) NULL,
                    [ADJ_TYPE_5]                        [VARCHAR] (2) NULL,
                    [ADJUSTOR_ID_5]                     [VARCHAR] (20) NULL,
                    [ADJ_CODE_6]                        [VARCHAR] (10) NULL,
                    [ADJ_TYPE_6]                        [VARCHAR] (2) NULL,
                    [ADJUSTOR_ID_6]                     [VARCHAR] (20) NULL,
                    [ADJ_CODE_7]                        [VARCHAR] (10) NULL,
                    [ADJ_TYPE_7]                        [VARCHAR] (2) NULL,
                    [ADJUSTOR_ID_7]                     [VARCHAR] (20) NULL,
                    [ADJ_CODE_8]                        [VARCHAR] (10) NULL,
                    [ADJ_TYPE_8]                        [VARCHAR] (2) NULL,
                    [ADJUSTOR_ID_8]                     [VARCHAR] (20) NULL,
                    [ADJ_CODE_9]                        [VARCHAR] (10) NULL,
                    [ADJ_TYPE_9]                        [VARCHAR] (2) NULL,
                    [ADJUSTOR_ID_9]                     [VARCHAR] (20) NULL,
                    [ADJ_CODE_10]                       [VARCHAR] (10) NULL,
                    [ADJ_TYPE_10]                       [VARCHAR] (2) NULL,
                    [ADJUSTOR_ID_10]                    [VARCHAR] (20) NULL,
                    [PAYER_BYPASS_CODE]                 [NUMERIC] (20) NULL,
                    [CDF_TEXT_1] 						[VARCHAR] (32) NULL,--old value [varchar](12) 2.3 changes
                    [CDF_TEXT_2]                        [VARCHAR] (32) NULL,
                    [CDF_TEXT_3]                        [VARCHAR] (32) NULL,
					--2.3 changes new fields start
					[INSURANCE_LOB_SUBTYPE] 			[VARCHAR](15) NULL, --V1.2
					[MEDICARE_PROVIDER_ID] 				[VARCHAR](80) NULL,
					[MEDICAID_PROVIDER_ID] 				[VARCHAR](80) NULL,
					[RENDERING_TAXONOMY_CODE] 			[VARCHAR](50) NULL,
					[BILLING_TAXONOMY_CODE] 			[VARCHAR](50) NULL,
					[SUB_NONCOVERED_AMOUNT] 			[NUMERIC](10, 2) NULL,
					[COB_AMOUNT] 						[NUMERIC](10, 2) NULL,
					[COB_COINSURANCE_AMOUNT] 			[NUMERIC](10, 2) NULL,
					[COB_DEDUCTIBLE_AMOUNT] 			[NUMERIC](10, 2) NULL,
					[COB_PAID_AMOUNT] 					[NUMERIC](10, 2) NULL,
					[COB_ALLOWED_AMOUNT] 				[NUMERIC](10, 2) NULL,
					[PAID_UNITS] 						[NUMERIC](13, 3) NULL,
					[LINE_CAPITATION_INDICATOR] 		[VARCHAR](1) NULL,
					[FEE_SERVICE_AMOUNT] 				[NUMERIC](10, 2) NULL,
					[PATIENT_LIABILITY_AMOUNT] 			[NUMERIC](10, 2) NULL,
					[DISALLOWED_AMOUNT] 				[NUMERIC](10, 2) NULL,
					[LINE_REIMBURSEMENT_TYPE] 			[VARCHAR](20) NULL,
					[REFERRING_TAXONOMY_CODE] 			[VARCHAR](50) NULL,
					[SUPERVISING_TAXONOMY_CODE] 		[VARCHAR](50) NULL,
					[ORDERING_TAXONOMY_CODE] 			[VARCHAR](50) NULL,
					[ADJUSTMENT_NUMBER] 				[VARCHAR](1) NULL,
					[ANESTHESIA_TIME] 					[NUMERIC](10) NULL,
					--2.3 changes new fields end
					[IS_ADJUSTED_CLAIM]					[VARCHAR] (3) NULL,
					[IS_VOIDED]							[VARCHAR] (3) NULL
                   PRIMARY KEY CLUSTERED
                   (
                   [CLAIM_LINE_FACT_KEY] ASC,
                   [CLAIM_ID] ASC,
                   [CLAIM_FACT_KEY] ASC
                   )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
                   ) ON [PRIMARY]


            INSERT INTO #ustil_hrp_stg_cotiviti_claimsLines_professional
            (
                [CLAIM_LINE_FACT_KEY],
                [CLAIM_ID],
                [CLAIM_FACT_KEY],
				[CLAIM_LINE_NUMBER_HRP],
				[CLAIM_LINE_NUMBER_COTIVITI],
                [INSURANCE_ID],
                [RENDERING_PROVIDER_ID],
                [RENDERING_TAXGROUP_ID],
                [RENDERING_NPI],
                [RENDERING_SUBSPEC_ID],
                [RENDERING_ENTITY_TYPE],
                [RENDERING_LAST_NAME],
                [RENDERING_FIRST_NAME],
                [RENDERING_MIDDLE_NAME],
                [RENDERING_NAME_SUFFIX],
                [RENDERING_STREET_ADDRESS_1],
                [RENDERING_STREET_ADDRESS_2],
                [RENDERING_CITY],
                [RENDERING_STATE],
                [RENDERING_ZIP],
                [RENDERING_ZIP_PLUS_4],
                [RENDERING_COUNTRY],
                [BILLING_PROVIDER_ID] ,
                [BILLING_TAXGROUP_ID],
                [BILLING_NPI],
                [BILLING_SUBSPEC_ID],
                [BILLING_ENTITY_TYPE],
                [BILLING_LAST_NAME] ,
                [BILLING_ADDRESS_1],
                [BILLING_ADDRESS_2],
                [BILLING_CITY],
                [BILLING_STATE],
                [BILLING_ZIP] ,
                [BILLING_ZIP_PLUS_4],
                [BILLING_COUNTRY],
                [LINE_SEQ],
                [DOS_FROM] ,
                [DOS_TO] ,
                [POS_ID] ,
                [TOS]  ,
                [DIAG_PTR_LIST],
                [SUB_HCPCS],
                [SUB_MOD_LIST],
                [UNITS_TYPE],
                [SUB_UNITS],
                [SUB_AMOUNT],
                [ALLOWED_HCPCS] ,
                [ALLOWED_UNITS],
                [ALLOWED_AMOUNT] ,
                [ALLOWED_NDC],
                [ALLOWED_NDC_UNITS],
                [ALLOWED_NDC_UNITS_TYPE] ,
                [COPAY],
                [COINSURANCE] ,
                [DEDUCTIBLE] ,
                [COB] ,
                [OTHER_REDUCTION]  ,
                [PAID]  ,
                [BYPASS_CODE],
                [PAR_YN] ,
                --[GROUP_NAME] ,--moved to header in 2.3
                [REFERRING_PROVIDER_ID],
                [REFERRING_NPI],
                [REFERRING_SUBSPEC_ID],
				[REFERRING_TAXONOMY_CODE],--2.3 changes
                [REFERRING_LAST_NAME],
                [REFERRING_FIRST_NAME],
                [REFERRING_MIDDLE_NAME],
                [REFERRING_NAME_SUFFIX],
				[IS_ADJUSTED_CLAIM],
				[IS_VOIDED],
				[RENDERING_TAXONOMY_CODE], --2.3 changes
				[BILLING_TAXONOMY_CODE], --2.3 changes
				[SUB_NONCOVERED_AMOUNT], --2.3 changes
				[SUB_NDC], --2.3 changes
				[SUB_NDC_UNITS], --2.3 changes
				[SUB_NDC_UNITS_TYPE],   --2.3 changes
				[PAID_UNITS], --2.3 changes
				[LINE_CAPITATION_INDICATOR], --2.3 changes
				[INSURANCE_LOB_SUBTYPE], --2.3 changes
				[MEDICARE_PROVIDER_ID], --2.3 changes
				[BILLING_FIRST_NAME] --2.3 changes
            )

            SELECT
                CL_FT.CLAIM_LINE_FACT_KEY    AS CLAIM_LINE_FACT_KEY,
                @lv_claim_hcc_id                AS CLAIM_ID,
                @ln_claim_fact_key            AS CLAIM_FACT_KEY,
				CL_FT.CLAIM_LINE_HCC_ID		 AS CLAIM_LINE_NUMBER_HRP,
				''							 AS	CLAIM_LINE_NUMBER_COTIVITI,
				'Medicare' AS INSURANCE_ID, --V1.2,V1.4
                COALESCE(PRACT.PRACTITIONER_HCC_ID, SUPP.SUPPLIER_HCC_ID ) AS RENDERING_PROVIDER_ID,
                COALESCE(PRACT.TAX_ID ,   SUPP.TAX_ID)  AS RENDERING_TAXGROUP_ID,
                COALESCE(PRACT.PRACTITIONER_NPI, SUPP.SUPPLIER_NPI ) AS RENDERING_NPI,
                COALESCE(PRACT.PROVIDER_TAXONOMY_CODE, SUPP.PROVIDER_TAXONOMY_CODE ) AS RENDERING_SUBSPEC_ID,
                'P' AS RENDERING_ENTITY_TYPE,
                COALESCE(PRACT.PRACTITIONER_LAST_NAME,  SUPP.SUPPLIER_NAME ) AS RENDERING_LAST_NAME,
                PRACT.PRACTITIONER_FIRST_NAME AS RENDERING_FIRST_NAME,
                PRACT.PRACTITIONER_MIDDLE_NAME AS RENDERING_MIDDLE_NAME,
                PRACT.PRACTITIONER_NAME_SUFFIX AS RENDERING_NAME_SUFFIX,
                IIF(PRACT.PRACTITIONER_HCC_ID IS NOT NULL, PRACT.ADDRESS_LINE,   SUPP.ADDRESS_LINE) AS  RENDERING_ADDRESS_01,
                IIF(PRACT.PRACTITIONER_HCC_ID IS NOT NULL, PRACT.ADDRESS_LINE_2, SUPP.ADDRESS_LINE_2) AS  RENDERING_ADDRESS_02,
                IIF(PRACT.PRACTITIONER_HCC_ID IS NOT NULL, PRACT.CITY_NAME,      SUPP.CITY_NAME) AS  RENDERING_CITY,
                IIF(PRACT.PRACTITIONER_HCC_ID IS NOT NULL, PRACT.STATE_CODE,     SUPP.STATE_CODE) AS RENDERING_STATE,
                IIF(PRACT.PRACTITIONER_HCC_ID IS NOT NULL, PRACT.ZIP_CODE,       SUPP.ZIP_CODE) AS RENDERING_ZIP,
                IIF(PRACT.PRACTITIONER_HCC_ID IS NOT NULL, PRACT.ZIP_4_CODE,     SUPP.ZIP_4_CODE) AS RENDERING_ZIP_PLUS_4,
                IIF(PRACT.PRACTITIONER_HCC_ID IS NOT NULL, PRACT.COUNTRY_CODE,   SUPP.COUNTRY_CODE) AS RENDERING_COUNTRY,
                SUPP.SUPPLIER_HCC_ID        AS BILLING_PROVIDER_ID,
                REPLACE(SUPP.TAX_ID, '-', '') AS BILLING_TAXGROUP_ID, --V1.3
                SUPP.SUPPLIER_NPI           AS BILLING_NPI,
                SUPP.PROVIDER_TAXONOMY_CODE AS BILLING_SUBSPEC_ID,
                'E'                         AS BILLING_ENTITY_TYPE,
                SUPP.SUPPLIER_NAME          AS BILLING_LAST_NAME,
                SUPP.ADDRESS_LINE           AS BILLING_ADDRESS_1,
                SUPP.ADDRESS_LINE_2         AS BILLING_ADDRESS_2,
                SUPP.CITY_NAME              AS BILLING_CITY,
                SUPP.STATE_CODE             AS BILLING_STATE,
                SUPP.ZIP_CODE               AS BILLING_ZIP,
                SUPP.ZIP_4_CODE             AS BILLING_ZIP_PLUS_4,
                SUPP.COUNTRY_CODE           AS BILLING_COUNTRY,
                ISNULL(RIGHT('0000' + CL_FT.CLAIM_LINE_HCC_ID,4),'') AS LINE_SEQ, --V1.3
                CL_FT_OTH.DOS_FROM          AS DOS_FROM,
                CL_FT_OTH.DOS_TO            AS DOS_TO,
                CL_FT.PLACE_OF_SERVICE_CODE AS POS_ID,
                ''          				AS TOS,
                CL_FT_OTH.DIAG_PTR          AS DIAG_PTR_LIST,
                CL_FT.SERVICE_CODE          AS SUB_HCPCS,
                CL_FT_OTH.MODIFIER_CODE     AS SUB_MOD_LIST,
                'UN'                        AS UNITS_TYPE,
                CL_FT_OTH.UNIT_COUNT        AS SUB_UNITS,
                CL_FT.BILLED_AMOUNT         AS SUB_AMOUNT,
                CL_FT.SERVICE_CODE          AS ALLOWED_HCPCS,
                IIF(CL_FT.CLAIM_LINE_STATUS_CODE IN ( 'd', 'i', 'r' ), 0, CL_FT.UNIT_COUNT ) AS ALLOWED_UNITS,--d-Denied, i-Invalid,r-Rejected
                CL_FT.BASE_ALLOWED_AMOUNT   AS ALLOWED_AMOUNT,
                REPLACE(CL_FT_OTH.ALLOWED_NDC, '-', '') AS ALLOWED_NDC, --V1.3
                CL_FT_OTH.ALLOWED_NDC_UNITS AS ALLOWED_NDC_UNITS,
                CL_FT_OTH.ALLOWED_NDC_UNITS_TYPE AS ALLOWED_NDC_UNITS_TYPE,
                CL_FT.BASE_COPAY_AMOUNT     AS COPAY,
                CL_FT.BASE_COINSURANCE_AMOUNT AS COINSURANCE,
                CL_FT.BASE_DEDUCTIBLE_AMOUNT AS DEDUCTIBLE,
                --CL_FT_OTH.COB               AS COB, --V1.5
				COALESCE(CL_FT_OTH.COB,0)               AS COB, --V1.5
                CL_FT.OTHER_DISCOUNT_AMOUNT AS OTHER_REDUCTION,
                CL_FT.PAID_AMOUNT           AS PAID,
				CASE WHEN UPPER(CL_FT.IS_ADJUSTED)='Y'
					 THEN
							CASE WHEN	ISNULL(CONVERT(VARCHAR(20),CL_FT.MANUAL_ALLOWED_AMOUNT),'') <> ''
							 		OR ISNULL(CONVERT(VARCHAR(20),CL_FT.MANUAL_BENEFIT_NETWORK_KEY),'') <> ''
									OR ISNULL(CONVERT(VARCHAR(20),CL_FT.MANUAL_REPRICER_KEY),'') <> ''
								 THEN 4
								 ELSE 0
							END
					  ELSE 0
				END							AS BYPASS_CODE,
                CL_FT_OTH.IS_NON_PAR        AS PAR_YN,
                --CL_FT_OTH.GROUP_NAME        AS GROUP_NAME,--moved to claim level in 2.3
                REFR.PRACTITIONER_HCC_ID    AS REFERRING_PROVIDER_ID,
                REFR.PRACTITIONER_NPI       AS REFERRING_NPI,
                REFR.PROVIDER_TAXONOMY_CODE AS REFERRING_SUBSPEC_ID,
				REFR.PROVIDER_TAXONOMY_CODE AS REFERRING_TAXONOMY_CODE,--2.3 changes
                REFR.PRACTITIONER_LAST_NAME AS REFERRING_LAST_NAME,
                REFR.PRACTITIONER_FIRST_NAME  AS REFERRING_FIRST_NAME,
                REFR.PRACTITIONER_MIDDLE_NAME AS REFERRING_MIDDLE_NAME,
                REFR.PRACTITIONER_NAME_SUFFIX AS REFERRING_NAME_SUFFIX,
				CL_FT.IS_ADJUSTED AS IS_ADJUSTED_CLAIM,
				CL_FT.IS_VOIDED AS IS_VOIDED,
				COALESCE(PRACT.PROVIDER_TAXONOMY_CODE, SUPP.PROVIDER_TAXONOMY_CODE ) AS RENDERING_TAXONOMY_CODE, --2.3 changes
				SUPP.PROVIDER_TAXONOMY_CODE AS BILLING_TAXONOMY_CODE, --2.3 changes
				CL_FT.BASE_NON_COVERED_AMOUNT         AS SUB_NONCOVERED_AMOUNT, --2.3 changes
				REPLACE(CL_FT_OTH.ALLOWED_NDC, '-', '') AS SUB_NDC, --2.3 changes --V1.3
                CL_FT_OTH.ALLOWED_NDC_UNITS AS SUB_NDC_UNITS, --2.3 changes
                CL_FT_OTH.ALLOWED_NDC_UNITS_TYPE AS SUB_NDC_UNITS_TYPE, --2.3 changes
				CL_FT.UNIT_COUNT AS PAID_UNITS, --2.3 changes
				(CASE WHEN CL_FT.LINE_CAPITATION_INDICATOR = 'Y'
						THEN 1
					  ELSE 0
				END), --2.3 changes --V1.3
				INS_LOB.LOB_PRODUCT_LINE AS INSURANCE_LOB_SUBTYPE,--2.3 changes --V1.2
				ISNULL(SPPLR_OTHER_ID.MEDICARE_PROVIDER_ID,'') AS MEDICARE_PROVIDER_ID, --2.3 changes
				SUPP.SUPPLIER_NAME          AS BILLING_FIRST_NAME --2.3 changes
              FROM  #ALL_CLAIM_LINE_FACT_TEMP CL_FT
                      INNER JOIN
                    #ALL_CLAIM_LINE_FACT_OTHER CL_FT_OTH ON
                      CL_FT_OTH.CLAIM_FACT_KEY = CL_FT.CLAIM_FACT_KEY
                      AND CL_FT_OTH.CLAIM_LINE_FACT_KEY = CL_FT.CLAIM_LINE_FACT_KEY
                      LEFT JOIN
                    #PRACTITIONER_TEMP PRACT ON
                      PRACT.PRACTITIONER_KEY = CL_FT.PRACTITIONER_KEY
                      AND PRACT.CLAIM_LINE_FACT_KEY = CL_FT.CLAIM_LINE_FACT_KEY
                       LEFT JOIN
                    #SUPPLIER_TEMP SUPP ON
                      SUPP.SUPPLIER_KEY = CL_FT.SUPPLIER_KEY
                      LEFT JOIN
                    #REFERING_TEMP REFR ON
                      REFR.REFERRING_PRACTITIONER_KEY = CL_FT.REFERRING_PRACTITIONER_KEY
					  LEFT JOIN --2.3 changes
					#SUPPLIER_OTHER_ID SPPLR_OTHER_ID ON--2.3 changes
						CL_FT.CLAIM_FACT_KEY = SPPLR_OTHER_ID.CLAIM_FACT_KEY --2.3 changes
						LEFT JOIN --2.3 changes
						#INSURANCE_LOB INS_LOB ON--2.3 changes
						CL_FT.CLAIM_FACT_KEY = INS_LOB.CLAIM_FACT_KEY--2.3 changes
								AND CL_FT.MEMBER_KEY = INS_LOB.MEMBER_KEY--2.3 changes
               WHERE
				NOT EXISTS (SELECT 1 FROM HRDW_REPLICA.PAYOR_DW.ALL_CLAIM_LINE_FACT CL_FT1
						WHERE
							CL_FT.CLAIM_FACT_KEY= CL_FT1.CLAIM_FACT_KEY
							AND CL_FT.CLAIM_LINE_FACT_KEY <> CL_FT1.CLAIM_LINE_FACT_KEY
							AND CL_FT.CLAIM_LINE_HCC_ID = CL_FT1.ORIGINAL_LINE_NUMBER
							AND UPPER(CL_FT1.IS_SPLIT) ='Y'
							AND ISNULL(UPPER(CL_FT1.DELETED_FLAG), '') <> 'Y')

                      SELECT *FROM #ustil_hrp_stg_cotiviti_claimsLines_professional
              END
          IF @lv_claim_type = 'Institutional'
          BEGIN

			DROP TABLE IF EXISTS #ustil_hrp_stg_cotiviti_claimsLines_institutional

            CREATE TABLE #ustil_hrp_stg_cotiviti_claimsLines_institutional
            (
              [CLAIM_LINE_FACT_KEY]         [NUMERIC] (19,0) NOT NULL,
              [CLAIM_ID]                    [VARCHAR] (25) NOT NULL,
              [CLAIM_FACT_KEY]              [NUMERIC] (19,0) NOT NULL,
			  [CLAIM_LINE_NUMBER_HRP]		[VARCHAR] (50)  NULL,
			  [CLAIM_LINE_NUMBER_COTIVITI]	[VARCHAR] (50)  NULL,
              [INSURANCE_ID]                [VARCHAR]  (15) NULL, --V1.2
              [BILLING_NPI]                 [NUMERIC]  (10) NULL,
              [BILLING_ZIP]                 [VARCHAR]  (5) NULL,
              [BILLING_ZIP_PLUS_4]          [VARCHAR]  (4) NULL,
              [CONTRACT_ID]                 [VARCHAR]  (25) NULL,
              [CONTRACT_TYPE_CODE]          [VARCHAR]  (2) NULL,
              [CONTRACT_AMOUNT]             [NUMERIC] (10,2) NULL,
              [CONTRACT_PCT]                [NUMERIC] (10,5) NULL,
              [CONTRACT_CODE]               [VARCHAR]  (50) NULL,
              [CONTRACT_TERMS_DISC_PCT]     [NUMERIC] (10,5) NULL,
              [CONTRACT_VERSION_ID]         [VARCHAR]  (30) NULL,
              [LINE_SEQ]                    [VARCHAR]  (50) NULL,
              [SUB_REV_CODE]                [VARCHAR]  (4) NULL,
              [SUB_HCPCS]                   [VARCHAR]  (5) NULL,
			  [SUB_MOD_LIST]                    [VARCHAR] (MAX) NULL, --2.3 changes
			  --commented for 2.3 changes start
             /* [SUB_MOD1]                    [VARCHAR]  (2) NULL,
              [SUB_MOD2]                    [VARCHAR]  (2) NULL,
              [SUB_MOD3]                    [VARCHAR]  (2) NULL,
              [SUB_MOD4]                    [VARCHAR]  (2) NULL,
              [SUB_MOD5]                    [VARCHAR]  (2) NULL,
              [SUB_MOD6]                    [VARCHAR]  (2) NULL,
              [SUB_MOD7]                    [VARCHAR]  (2) NULL,
              [SUB_MOD8]                    [VARCHAR]  (2) NULL,
			  */
			  --commented for 2.3 changes end
              [DOS_FROM]                    [DATE]      NULL,
              [UNITS_TYPE]                  [VARCHAR]  (2) NULL,
              [SUB_UNITS]                   [NUMERIC] (19,5) NULL,
              [SUB_AMOUNT]                  [NUMERIC] (10,2) NULL,
              [SUB_NON_COVERED_AMOUNT]      [NUMERIC] (10,2) NULL,
              [ALLOWED_REV_CODE]            [VARCHAR]  (4) NULL,
              [ALLOWED_HCPCS]               [VARCHAR]  (5) NULL,
              [ALLOWED_MOD1]                [VARCHAR]  (2) NULL,
              [ALLOWED_MOD2]                [VARCHAR]  (2) NULL,
              [ALLOWED_MOD3]                [VARCHAR]  (2) NULL,
              [ALLOWED_MOD4]                [VARCHAR]  (2) NULL,
              [ALLOWED_MOD5]                [VARCHAR]  (2) NULL,
              [ALLOWED_MOD6]                [VARCHAR]  (2) NULL,
              [ALLOWED_MOD7]                [VARCHAR]  (2) NULL,
              [ALLOWED_MOD8]                [VARCHAR]  (2) NULL,
              [ALLOWED_UNITS]               [NUMERIC] (19,5) NULL,
              [ALLOWED_AMOUNT]              [NUMERIC] (10,2) NULL,
              [SUB_NDC]                     [VARCHAR]  (11) NULL,
              [SUB_NDC_UNITS]               [NUMERIC] (19,5) NULL,
              [SUB_NDC_UNITS_TYPE]          [VARCHAR]  (2) NULL,
              [COMPOUND_DRUG_YN]            [VARCHAR]  (1) NULL,
              [ALLOWED_NDC]                 [VARCHAR]  (13) NULL,
              [ALLOWED_NDC_UNITS]           [NUMERIC] (19,5) NULL,
              [ALLOWED_NDC_UNITS_TYPE]      [VARCHAR]  (2) NULL,
              [COPAY]                       [NUMERIC] (10,2) NULL,
              [COINSURANCE]                 [NUMERIC] (10,2) NULL,
              [DEDUCTIBLE]                  [NUMERIC] (10,2) NULL,
              [COB]                         [NUMERIC] (10,2) NULL,
              [OTHER_REDUCTION]             [NUMERIC] (10,2) NULL,
              [PAID]                        [NUMERIC] (10,2) NULL,
              [PAID_DATE]                   [DATE]      NULL,
              [BYPASS_CODE]                 [NUMERIC]  (20) DEFAULT 0,
              [PAR_YN]                      [VARCHAR]  (1) NULL,
              [EDIT_0_ALLOWED_YN]           [VARCHAR]  (1) NULL,
              [ZERO_CHG_LINE_SEQ]           [VARCHAR]  (50) NULL,
              [ZERO_CHG_SCHED_AMOUNT]       [NUMERIC] (10,2) NULL,
              [LINE_SEQ_ORIG]               [VARCHAR]  (50) NULL,
              [ADJ_CODE_1]                  [VARCHAR]  (10) NULL,
              [ADJ_TYPE_1]                  [VARCHAR]  (2) NULL,
              [ADJUSTOR_ID_1]               [VARCHAR]  (20) NULL,
              [ADJ_CODE_2]                  [VARCHAR]  (10) NULL,
              [ADJ_TYPE_2]                  [VARCHAR]  (2) NULL,
              [ADJUSTOR_ID_2]               [VARCHAR]  (20) NULL,
              [ADJ_CODE_3]                  [VARCHAR]  (10) NULL,
              [ADJ_TYPE_3]                  [VARCHAR]  (2) NULL,
              [ADJUSTOR_ID_3]               [VARCHAR]  (20) NULL,
              [ADJ_CODE_4]                  [VARCHAR]  (10) NULL,
              [ADJ_TYPE_4]                  [VARCHAR]  (2) NULL,
              [ADJUSTOR_ID_4]               [VARCHAR]  (20) NULL,
              [ADJ_CODE_5]                  [VARCHAR]  (10) NULL,
              [ADJ_TYPE_5]                  [VARCHAR]  (2) NULL,
              [ADJUSTOR_ID_5]               [VARCHAR]  (20) NULL,
              [ADJ_CODE_6]                  [VARCHAR]  (10) NULL,
              [ADJ_TYPE_6]                  [VARCHAR]  (2) NULL,
              [ADJUSTOR_ID_6]               [VARCHAR]  (20) NULL,
              [ADJ_CODE_7]                  [VARCHAR]  (10) NULL,
              [ADJ_TYPE_7]                  [VARCHAR]  (2) NULL,
              [ADJUSTOR_ID_7]               [VARCHAR]  (20) NULL,
              [ADJ_CODE_8]                  [VARCHAR]  (10) NULL,
              [ADJ_TYPE_8]                  [VARCHAR]  (2) NULL,
              [ADJUSTOR_ID_8]               [VARCHAR]  (20) NULL,
              [ADJ_CODE_9]                  [VARCHAR]  (10) NULL,
              [ADJ_TYPE_9]                  [VARCHAR]  (2) NULL,
              [ADJUSTOR_ID_9]               [VARCHAR]  (20) NULL,
              [ADJ_CODE_10]                 [VARCHAR]  (10) NULL,
              [ADJ_TYPE_10]                 [VARCHAR]  (2) NULL,
              [ADJUSTOR_ID_10]              [VARCHAR]  (20) NULL,
              [PAYER_BYPASS_CODE]           [NUMERIC]  (20) NULL,
              [CDF_TEXT_1] 					[VARCHAR]  (32) NULL,--old value [varchar](12)
              [CDF_TEXT_2]                  [VARCHAR]  (32) NULL,
              [CDF_TEXT_3]                  [VARCHAR]  (32) NULL,
              [ANESTHESIA_TIME]             [NUMERIC]  (10) NULL,
              [CLIENT_TYPE_OF_SERVICE]      [VARCHAR]  (20) NULL,
              [LINE_FEE_SERVICE_AMOUNT]     [NUMERIC] (10,2) NULL,
              [LINE_PATIENT_LIAB_AMOUNT]    [NUMERIC] (10,2) NULL,
              [LINE_REIMBURSEMENT_TYPE]     [VARCHAR]  (20) NULL,
              [MEDICARE_SERVICE_FEE_AMOUNT] [NUMERIC] (10,2) NULL,
              [DOS_TO]                      [DATE]      NULL,
			  --2.3 changes new fields start
			  [APC_CODE] 					[NUMERIC](4, 0) NULL,
			  [APC_PAYMENT_WEIGHT] 			[NUMERIC](7, 3) NULL,
			  [APC_VERSION] 				[VARCHAR](5) NULL,
			  [APG_CODE] 					[VARCHAR](5) NULL,
			  [APG_VERSION] 				[VARCHAR](5) NULL,
			  [INSURANCE_LOB_SUBTYPE] 		[VARCHAR](15) NULL,--cotiviti [VARCHAR](15) --V1.2
			  [COB_AMOUNT] 					[NUMERIC](19, 2) NULL,--cotiviti [NUMERIC](10, 2)
			  [COB_COINSURANCE_AMOUNT] 		[NUMERIC](19, 2) NULL,--cotiviti [NUMERIC](10, 2)
			  [COB_DEDUCTIBLE_AMOUNT] 		[NUMERIC](19, 2) NULL,--cotiviti [NUMERIC](10, 2)
			  [COB_PAID_AMOUNT] 			[NUMERIC](19, 2) NULL,--cotiviti [NUMERIC](10, 2)
			  [COB_ALLOWED_AMOUNT] 			[NUMERIC](19, 2) NULL,--cotiviti [NUMERIC](10, 2)
			  [PAID_UNITS] 					[NUMERIC](19, 5) NULL,--cotiviti[NUMERIC](13, 3)
			  [LINE_CAPITATION_INDICATOR] 	[VARCHAR](3) NULL,--cotiviti[VARCHAR](1)
			  [DISALLOWED_AMOUNT] 			[NUMERIC](10, 2) NULL,
			  [ADJUSTMENT_NUMBER] 			[VARCHAR](1) NULL,
			  [EMERGENCY_INDICATOR_YN] 		[VARCHAR](1) NULL,
			  --2.3 changes new fields end
			  [IS_ADJUSTED_CLAIM]			[VARCHAR] (3) NULL,
			  [IS_VOIDED]					[VARCHAR] (3) NULL,

              PRIMARY KEY CLUSTERED
               (
                 [CLAIM_LINE_FACT_KEY] ASC,
                 [CLAIM_FACT_KEY] ASC,
                 [CLAIM_ID] ASC
                 )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
               ) ON [primary]

                INSERT INTO #ustil_hrp_stg_cotiviti_claimsLines_institutional
                (
                    [CLAIM_LINE_FACT_KEY],
                    [CLAIM_ID],
                    [CLAIM_FACT_KEY],
					[CLAIM_LINE_NUMBER_HRP],
					[CLAIM_LINE_NUMBER_COTIVITI],
                    [INSURANCE_ID],
					[BILLING_NPI],--2.3 changes
                    [LINE_SEQ],
                    [SUB_REV_CODE],
                    [DOS_FROM],
                    [SUB_UNITS],
                    [SUB_AMOUNT],
                    [ALLOWED_REV_CODE],
					[ALLOWED_HCPCS],	--V1.3
                    [ALLOWED_UNITS],
                    [ALLOWED_AMOUNT],
                    [COPAY],
                    [COINSURANCE],
                    [DEDUCTIBLE],
                    [COB],
                    [OTHER_REDUCTION] ,
                    [PAID],
                    [BYPASS_CODE],
                    [PAR_YN],
                    [DOS_TO],
					[IS_ADJUSTED_CLAIM],
					[IS_VOIDED],
					[SUB_HCPCS], --2.3 changes
				    [SUB_MOD_LIST],  --2.3 changes
					[UNITS_TYPE], --2.3 changes
					[SUB_NON_COVERED_AMOUNT], --2.3 changes
					[SUB_NDC], --2.3 changes
					[SUB_NDC_UNITS], --2.3 changes
					[SUB_NDC_UNITS_TYPE],   --2.3 changes
					[ALLOWED_NDC],   --2.3 changes
					[ALLOWED_NDC_UNITS],   --2.3 changes
					[ALLOWED_NDC_UNITS_TYPE],   --2.3 changes
					[INSURANCE_LOB_SUBTYPE],   --2.3 changes
					[PAID_UNITS], --2.3 changes
					[LINE_CAPITATION_INDICATOR] --2.3 changes
                  )

                SELECT
					CL_FT.CLAIM_LINE_FACT_KEY	  AS CLAIM_LINE_FACT_KEY,
					@lv_claim_hcc_id               AS CLAIM_ID,
					@ln_claim_fact_key           AS CLAIM_FACT_KEY,
					CL_FT.CLAIM_LINE_HCC_ID	  AS CLAIM_LINE_NUMBER_HRP,
					''						  AS CLAIM_LINE_NUMBER_COTIVITI,
					'Medicare' AS INSURANCE_ID, --V1.2,V1.4
					SUPP.SUPPLIER_NPI           AS BILLING_NPI,--2.3 changes
					ISNULL(RIGHT('0000' + CL_FT.CLAIM_LINE_HCC_ID,4),'') AS LINE_SEQ, --V1.3
					CL_FT.REVENUE_CODE          AS SUB_REV_CODE,
					CL_FT_OTH.DOS_FROM          AS DOS_FROM,
					CL_FT_OTH.UNIT_COUNT        AS SUB_UNITS,
					CL_FT.BILLED_AMOUNT         AS SUB_AMOUNT,
					CL_FT.REVENUE_CODE          AS ALLOWED_REV_CODE,
					CL_FT.SERVICE_CODE          AS ALLOWED_HCPCS, --V1.3
					IIF(CL_FT.CLAIM_LINE_STATUS_CODE IN ( 'd', 'i', 'r' ), 0, CL_FT.UNIT_COUNT ) AS ALLOWED_UNITS,--d-Denied, i-Invalid,r-Rejected
					CL_FT.BASE_ALLOWED_AMOUNT   AS ALLOWED_AMOUNT,
					CL_FT.BASE_COPAY_AMOUNT     AS COPAY,
					CL_FT.BASE_COINSURANCE_AMOUNT AS COINSURANCE,
					CL_FT.BASE_DEDUCTIBLE_AMOUNT AS DEDUCTIBLE,
					--CL_FT_OTH.COB               AS COB --V1.5
					COALESCE(CL_FT_OTH.COB,0)               AS COB, --V1.5
					CL_FT.OTHER_DISCOUNT_AMOUNT AS OTHER_REDUCTION,
					CL_FT.PAID_AMOUNT           AS PAID,
					CASE WHEN UPPER(CL_FT.IS_ADJUSTED)='Y'
					 THEN
							CASE WHEN	ISNULL(CONVERT(VARCHAR(20),CL_FT.MANUAL_ALLOWED_AMOUNT),'') <> ''
									OR ISNULL(CONVERT(VARCHAR(20),CL_FT.MANUAL_BENEFIT_NETWORK_KEY),'') <> ''
									OR ISNULL(CONVERT(VARCHAR(20),CL_FT.MANUAL_REPRICER_KEY),'') <> ''
								 THEN 4
								 ELSE 0
							END
					  ELSE 0
					END						  AS BYPASS_CODE,
					CL_FT_OTH.IS_NON_PAR        AS PAR_YN,
					CL_FT_OTH.DOS_TO            AS DOS_TO,
					CL_FT.IS_ADJUSTED AS IS_ADJUSTED_CLAIM,
					CL_FT.IS_VOIDED AS IS_VOIDED,
					CL_FT.SERVICE_CODE          AS SUB_HCPCS, --2.3 changes
					CL_FT_OTH.MODIFIER_CODE     AS SUB_MOD_LIST, --2.3 changes
					'UN'                        AS UNITS_TYPE, --2.3 changes
					CL_FT.BASE_NON_COVERED_AMOUNT AS SUB_NON_COVERED_AMOUNT, --2.3 changes
					REPLACE(CL_FT_OTH.ALLOWED_NDC, '-','') AS SUB_NDC, --2.3 changes --V1.3
					CL_FT_OTH.ALLOWED_NDC_UNITS AS SUB_NDC_UNITS, --2.3 changes
					CL_FT_OTH.ALLOWED_NDC_UNITS_TYPE AS SUB_NDC_UNITS_TYPE, --2.3 changes
					REPLACE(CL_FT_OTH.ALLOWED_NDC, '-', '') AS ALLOWED_NDC, --2.3 changes --V1.3
					CL_FT_OTH.ALLOWED_NDC_UNITS AS ALLOWED_NDC_UNITS, --2.3 changes
					CL_FT_OTH.ALLOWED_NDC_UNITS_TYPE AS ALLOWED_NDC_UNITS_TYPE, --2.3 changes
					INS_LOB.LOB_PRODUCT_LINE AS INSURANCE_LOB_SUBTYPE, --2.3 changes --V1.2
					CL_FT.UNIT_COUNT AS PAID_UNITS, --2.3 changes
					(CASE WHEN CL_FT.LINE_CAPITATION_INDICATOR = 'Y'
							THEN 1
						  ELSE 0
					END) --2.3 changes --V1.3
                FROM  #ALL_CLAIM_LINE_FACT_TEMP CL_FT
                        INNER JOIN
                      #ALL_CLAIM_LINE_FACT_OTHER CL_FT_OTH ON
                        CL_FT_OTH.CLAIM_FACT_KEY = CL_FT.CLAIM_FACT_KEY
                        AND CL_FT_OTH.CLAIM_LINE_FACT_KEY = CL_FT.CLAIM_LINE_FACT_KEY
						LEFT JOIN--2.3 changes
					  #SUPPLIER_TEMP SUPP ON--2.3 changes
						SUPP.SUPPLIER_KEY = CL_FT.SUPPLIER_KEY --2.3 changes
						LEFT JOIN --2.3 changes
						#INSURANCE_LOB INS_LOB ON--2.3 changes
						CL_FT.CLAIM_FACT_KEY = INS_LOB.CLAIM_FACT_KEY--2.3 changes
								AND CL_FT.MEMBER_KEY = INS_LOB.MEMBER_KEY--2.3 changes
                WHERE
					NOT EXISTS (SELECT 1 FROM HRDW_REPLICA.PAYOR_DW.ALL_CLAIM_LINE_FACT CL_FT1
						WHERE
							CL_FT.CLAIM_FACT_KEY= CL_FT1.CLAIM_FACT_KEY
						AND CL_FT.CLAIM_LINE_FACT_KEY <> CL_FT1.CLAIM_LINE_FACT_KEY
						AND CL_FT.CLAIM_LINE_HCC_ID = CL_FT1.ORIGINAL_LINE_NUMBER
						AND UPPER(CL_FT1.IS_SPLIT) ='Y'
						AND ISNULL(UPPER(CL_FT1.DELETED_FLAG), '') <> 'Y')

                SELECT * FROM #ustil_hrp_stg_cotiviti_claimsLines_institutional
              END


     --DEBUG-- PRINT ' TO GET FETCHING FINAL RESULT SET Completed : ' + CONVERT( varchar, Getdate(),121)

    DROP TABLE IF EXISTS #ALL_CLAIM_FACT_TEMP
    DROP TABLE IF EXISTS #ALL_CLAIM_LINE_FACT_TEMP
    DROP TABLE IF EXISTS #ALL_CLAIM_LINE_FACT_OTHER
    DROP TABLE IF EXISTS #PRACTITIONER_TEMP
    DROP TABLE IF EXISTS #SUPPLIER_TEMP
    DROP TABLE IF EXISTS #ustil_hrp_stg_cotiviti_claimsLines_institutional
    DROP TABLE IF EXISTS #ustil_hrp_stg_cotiviti_claimsLines_professional
	DROP TABLE IF EXISTS #REFERING_TEMP
	DROP TABLE IF EXISTS #SUPPLIER_OTHER_ID --2.3 changes
	DROP TABLE IF EXISTS #INSURANCE_LOB --2.3 changes

  END TRY
  BEGIN CATCH

			SELECT
               @li_return_code = @@ERROR

            IF @li_return_code <> 0
                BEGIN
                    SELECT
                        @lv_Msg_Desc            = 'Error Encountered : ' + CAST(Error_Number() AS VARCHAR) + ' : ' + Error_Message()
                                                   + ' at Line ' + CAST(Error_Line() AS VARCHAR) + ' in sp : ' + Error_Procedure(),
						            @lv_Msg_Type    		= 'ERR',
						            @ldt_Log_DTM    		= GETDATE()
						PRINT @lv_Msg_Desc
                END

    END CATCH

        PRINT '================================================================='

        SELECT
            @lv_Msg_Desc = 'END PROCESS   dbo.USP_cotiviti_daily_claim_line_cds_insertion_impl at ' + CONVERT(CHAR(27), GETDATE(), 109)

        PRINT @lv_Msg_Desc

        PRINT '================================================================='

        SELECT
            @lv_Msg_Desc = 'Return code from  dbo.USP_cotiviti_daily_claim_line_cds_insertion_impl :  ' + CAST(@li_return_code AS VARCHAR)

        PRINT @lv_Msg_Desc

    END