USE [INTEGRATION_PLUS_DB]
GO
/****** Object:  StoredProcedure [dbo].[USP_cotiviti_daily_initial_sweep_impl]    Script Date: 7/28/2023 3:53:45 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author          : UST OFFSHORE
-- Create date     : 04/11/2022
-- Description     : To retrive the Claim Details which are in 'Waiting for External System' from HRDW
-- Tables involved : 
--                   ALL_CLAIM_FACT
--=============================================
--Version History
--Date             Changed by          Description
--04/11/2022       UST Offshore       Initial creation

CREATE OR ALTER PROCEDURE [dbo].[USP_cotiviti_daily_initial_sweep_impl]
(
	@pSTART_TIME DATETIME,
	@pEND_TIME DATETIME
)
AS
BEGIN
 
	SET ANSI_NULLS ON
	
	SET QUOTED_IDENTIFIER ON
	
	SET NOCOUNT ON
  
  DECLARE
		 @lv_return_code    INT
		,@lv_Msg_Desc       VARCHAR(900)
		,@ldt_Log_DTM       DATETIME
		,@lv_Msg_Type       VARCHAR(50)
		,@ldt_start_time			DATETIME
		,@ldt_end_time				DATETIME
 
  BEGIN TRY

/* INITIALIZATION OF VARIABLE */

	SET @ldt_start_time = @pSTART_TIME
	SET @ldt_end_time = @pEND_TIME

/*INITIAL SWEEP QUERY TO FETCH THE CLAIMS WHICH ARE IN  'Waiting For External System' */

	DROP TABLE IF EXISTS #CLAIM_ID
	CREATE TABLE #CLAIM_ID
	(
	 CLAIM_HCC_ID	VARCHAR(30)
	)
	
	DROP TABLE IF EXISTS #CLAIM_FACT_WITH_ROW_NO  
	CREATE TABLE #CLAIM_FACT_WITH_ROW_NO  
	(  
	 CLAIM_FACT_KEY		NUMERIC(19)  
	,CLAIM_HCC_ID		VARCHAR(30)  
	,CLAIM_TYPE_NAME	VARCHAR(50)  
	,CLAIM_STATUS		VARCHAR(50)  
	,ROW_NO				INT  
	)  

	INSERT INTO #CLAIM_ID
	SELECT
		DISTINCT CLAIM.CLAIM_HCC_ID
	FROM HRDW_REPLICA.PAYOR_DW.ALL_CLAIM_FACT CLAIM WITH(NOLOCK)
	WHERE
		CLAIM.ENDORSEMENT_EFF_TIME BETWEEN @ldt_start_time AND @ldt_end_time
		AND CLAIM.ENDORSEMENT_EXPIRE_TIME > GETDATE()
		AND ISNULL(UPPER(CLAIM.DELETED_FLAG), '') <> 'Y'
		AND UPPER(CLAIM.IS_CURRENT)='Y'
		AND UPPER(CLAIM.CLAIM_STATUS) = 'WAITING FOR EXTERNAL SYSTEM'
			
	INSERT INTO #CLAIM_FACT_WITH_ROW_NO
	SELECT
	CLAIM.CLAIM_FACT_KEY
	,CLAIM.CLAIM_HCC_ID
	,CLAIM.CLAIM_TYPE_NAME
	,CLAIM.CLAIM_STATUS
	,ROW_NUMBER() OVER(PARTITION BY CLAIM.CLAIM_HCC_ID ORDER BY CLAIM.CLAIM_FACT_KEY DESC) AS ROW_NO
	FROM HRDW_REPLICA.PAYOR_DW.ALL_CLAIM_FACT CLAIM WITH(NOLOCK)
	INNER JOIN #CLAIM_ID CLAIM_TEMP
	ON CLAIM.CLAIM_HCC_ID=CLAIM_TEMP.CLAIM_HCC_ID
	WHERE
		ENDORSEMENT_EXPIRE_TIME > GETDATE()
	AND ISNULL(UPPER(CLAIM.DELETED_FLAG), '') <> 'Y'
	AND CLAIM.IS_CURRENT='Y'
	
	SELECT DISTINCT CLAIM_FACT_KEY,CLAIM_HCC_ID,UPPER(CLAIM_TYPE_NAME) AS CLAIM_TYPE_NAME
	FROM #CLAIM_FACT_WITH_ROW_NO
	WHERE
	ROW_NO = 1
	AND UPPER(CLAIM_STATUS) = 'WAITING FOR EXTERNAL SYSTEM'

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
            @lv_Msg_Desc = 'END PROCESS   dbo.USP_cotiviti_daily_initial_sweep_impl at ' + CONVERT(CHAR(27), GETDATE(), 109)

        PRINT @lv_Msg_Desc

        PRINT '================================================================='

        SELECT
            @lv_Msg_Desc = 'Return code from  dbo.USP_cotiviti_daily_initial_sweep_impl :  ' + CAST(@lv_return_code AS VARCHAR)

        PRINT @lv_Msg_Desc
       
 END