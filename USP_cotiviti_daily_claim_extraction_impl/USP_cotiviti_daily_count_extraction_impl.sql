USE [INTEGRATION_PLUS_DB]
GO
/****** Object:  StoredProcedure [dbo].[USP_cotiviti_daily_count_extraction_impl]    Script Date: 7/28/2023 3:53:30 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author          : UST OFFSHORE
-- Create date     : 04/08/2022
-- Description     : To retrive the Claim Details from HRDW for the given calim fact key to load into CDS tables
-- Tables involved :
--                   ustil_hrp_stg_cotiviti_claims_institutional
--					 ustil_hrp_stg_cotiviti_claims_professional
--					 ustil_hrp_stg_cotiviti_audit_report_tbl
--=============================================
--Version History
--Version		Date             Changed by          Description
--	1.0		04/08/2022			UST Offshore       Initial creation
--	1.1		12/07/2023			UST Offshore       Datetime logic changed to pull missing claims in extract

CREATE OR ALTER PROCEDURE [dbo].[USP_cotiviti_daily_count_extraction_impl]
(
  @pSTART_TIME DATETIME
 ,@pEND_TIME DATETIME
)
AS
 BEGIN

	SET ANSI_NULLS ON

	SET QUOTED_IDENTIFIER ON

	SET NOCOUNT ON
                               /*** VARIABLE DECLARATION ***/
  DECLARE
		 @lv_return_code			INT
		,@lv_Msg_Desc             	VARCHAR(900)
		,@ldt_Log_DTM              	DATETIME
		,@lv_Msg_Type              	VARCHAR(50)
		,@dt_start_time			 	DATETIME
		,@dt_end_time				DATETIME

  BEGIN TRY

	SET @dt_start_time=@pSTART_TIME
	SET @dt_end_time=@pEND_TIME

	/* CREATING TEMPERORY TABLE TO LOAD THE CLAIMS COUNT */

	DROP TABLE IF EXISTS #CLAIMS_COUNT
	CREATE TABLE #CLAIMS_COUNT
	(
	  CLAIM_TYPE VARCHAR(20)
	 ,TOTAL_CLAIMS_COUNT NUMERIC(19)
	 ,STARTING_RECORD_NUMBER NUMERIC(19)
	)

	INSERT INTO #CLAIMS_COUNT
	SELECT
		 'PROFESSIONAL' AS CLAIM_TYPE
		,COUNT(prof.CLAIM_FACT_KEY) AS TOTAL_CLAIMS_COUNT
		,MIN (prof.SEQ_NO) AS STARTING_RECORD_NUMBER
	FROM integrationPlus.ustil_hrp_stg_cotiviti_claims_professional prof
	INNER JOIN integrationPlus.ustil_hrp_stg_cotiviti_audit_report_tbl aud
	ON prof.CLAIM_FACT_KEY=aud.claim_number
	WHERE
		UPPER(aud.insert_status)='SUCCESS'
	AND ISNULL(aud.extract_status,'')=''
	--AND prof.ENDORSEMENT_EFF_TIME BETWEEN @dt_start_time AND @dt_end_time --V1.1 commented
	AND prof.CREATED_DATE BETWEEN @dt_start_time AND @dt_end_time --V1.1 added

	INSERT INTO #CLAIMS_COUNT
	SELECT
		 'INSTITUTIONAL' AS CLAIM_TYPE
		,COUNT(inst.CLAIM_FACT_KEY) AS TOTAL_CLAIMS_COUNT
		,MIN (inst.SEQ_NO) AS STARTING_RECORD_NUMBER
	FROM integrationPlus.ustil_hrp_stg_cotiviti_claims_institutional inst
	INNER JOIN integrationPlus.ustil_hrp_stg_cotiviti_audit_report_tbl aud
	ON inst.CLAIM_FACT_KEY=aud.claim_number
	WHERE
		UPPER(aud.insert_status)='SUCCESS'
	AND ISNULL(aud.extract_status,'')=''
	--AND inst.ENDORSEMENT_EFF_TIME BETWEEN @dt_start_time AND @dt_end_time --V1.1 commented
	AND inst.CREATED_DATE BETWEEN @dt_start_time AND @dt_end_time --V1.1 added

	SELECT * FROM #CLAIMS_COUNT

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
					PRINT @lv_Msg_Desc
                END

    END CATCH

        PRINT '================================================================='

        SELECT
            @lv_Msg_Desc = 'END PROCESS   USP_cotiviti_daily_count_extraction_impl at ' + CONVERT(CHAR(27), GETDATE(), 109)

        PRINT @lv_Msg_Desc

        PRINT '================================================================='

        SELECT
            @lv_Msg_Desc = 'Return code from  USP_cotiviti_daily_count_extraction_impl :  ' + CAST(@lv_return_code AS VARCHAR)

        PRINT @lv_Msg_Desc

    END