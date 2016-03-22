CREATE OR REPLACE PACKAGE PKG_LTE_CORE AS 

  /**
  * Author: Carrasco Marcelo
  * Date: 11/02/2016
  * Comment: Paquete para la visualizaci?n de datos de LTE
  */

  /**
  *
  */
  FUNCTION F_FLNS_3100A(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED;
  
  /**
  *
  */
  FUNCTION F_FLNS_3101A(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED;
  
  /**
  *
  */
  FUNCTION F_FLNS_3111A(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED;
  /**
  *
  */
  FUNCTION F_FLNS_3112A(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED;
  /**
  *
  */
  FUNCTION F_FLNS_3172A(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED;
  /**
  *
  */
  FUNCTION F_FLNS_3102D(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED;
  /**
  *
  */
  FUNCTION F_FLNS_3113A(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED;
  /**
  *
  */
  FUNCTION F_FLNS_3103B(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED;
  /**
  *
  */
  FUNCTION F_FLNS_3104B(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED;
  /**
  *
  */
  FUNCTION F_FLNS_3114A(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED;
  /**
  *
  */
  FUNCTION F_FLNS_3105B(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED;
  /**
  *
  */
  FUNCTION F_FLNS_3106A(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED;
  /**
  *
  */
  FUNCTION F_FLNS_3218A(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED;
  /**
  *
  */
  FUNCTION F_FLNS_3115A(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED;
  /**
  *
  */
  FUNCTION F_FLNS_3108B(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED;
  /**
  *
  */
  FUNCTION F_FLNS_3109B(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB2 PIPELINED;
  
  FUNCTION F_FLNS_3287A(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB2 PIPELINED;
  
  FUNCTION F_CONSOLIDADO (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED;

END PKG_LTE_CORE;
/


CREATE OR REPLACE PACKAGE BODY PKG_LTE_CORE AS

  FUNCTION F_FLNS_3100A(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED AS
  
  BEGIN
    for I in (select  /*+ INDEX_RS_ASC(PCOFNS_PS_MMMT_TA_RAW) */ PERIOD_START_TIME FECHA, 
                      PERIOD_DURATION, 
                      FINS_ID,
                      TA_ID,
                      sum(EPS_ATTACH_SUCC + EPS_ATTACH_FAIL) AS FLNS_3100A
              from PCOFNS_PS_MMMT_TA_RAW
              where PERIOD_START_TIME between TO_DATE(P_FINI,'dd.mm.yyyy') 
              AND to_date(P_FFIN,'dd.mm.yyyy')      
              GROUP BY PERIOD_START_TIME, PERIOD_DURATION, FINS_ID, TA_ID  
              ) LOOP
      pipe row (FLNS_OBJ(i.FECHA,i.PERIOD_DURATION,i.FINS_ID,i.TA_ID,i.FLNS_3100A));
    end LOOP;
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_FLNS_3100A',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
  end F_FLNS_3100A;

  FUNCTION F_FLNS_3101A(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED AS
  BEGIN
    FOR I IN (SELECT  /*+ INDEX_RS_ASC(PCOFNS_PS_MMMT_TA_RAW) */ 
                      PERIOD_START_TIME FECHA, 
                      PERIOD_DURATION,
                      FINS_ID,
                      TA_ID,
                      sum(EPS_DETACH) AS FLNS_3101A
              from PCOFNS_PS_MMMT_TA_RAW
              WHERE PERIOD_START_TIME BETWEEN TO_DATE(P_FINI,'dd.mm.yyyy') 
              AND TO_DATE(P_FFIN,'dd.mm.yyyy')
              GROUP BY PERIOD_START_TIME, PERIOD_DURATION, FINS_ID, TA_ID
              ) LOOP
      pipe row (FLNS_OBJ(i.FECHA,i.PERIOD_DURATION,i.FINS_ID,i.TA_ID,i.FLNS_3101A));
    end LOOP;
    
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_FLNS_3101A',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
  end F_FLNS_3101A;

  FUNCTION F_FLNS_3111A(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED AS
  BEGIN
  
    for I in (SELECT  /*+ INDEX_RS_ASC(PCOFNS_PS_SMMT_TA_RAW) */ PERIOD_START_TIME FECHA,
                      PERIOD_DURATION, 
                      FINS_ID,
                      TA_ID,
                      sum(PDN_CONNECTIVITY_FAILED_UE +
                          PDN_CONNECTIVITY_FAILED_ENB +
                          DDBEARER_UEINIT_ACT_SUCC +
                          DDBEARER_UEINIT_ACT_FAIL +
                          DDBEARER_PGWINIT_ACT_SUCC +
                          DDBEARER_PGWINIT_ACT_FAIL) AS FLNS_3111A
              from PCOFNS_PS_SMMT_TA_RAW
              WHERE PERIOD_START_TIME BETWEEN TO_DATE(P_FINI,'dd.mm.yyyy') 
              AND to_date(P_FFIN,'dd.mm.yyyy')
              GROUP BY PERIOD_START_TIME, PERIOD_DURATION, FINS_ID, TA_ID
              ) LOOP
      pipe row (FLNS_OBJ(i.FECHA,i.PERIOD_DURATION,i.FINS_ID,i.TA_ID,i.FLNS_3111A));
    end LOOP;
  
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_FLNS_3111A',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
        
  END F_FLNS_3111A;
  
  FUNCTION F_FLNS_3112A(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED AS
  BEGIN
  
    for I in (SELECT  /*+ INDEX_RS_ASC(PCOFNS_PS_SMMT_TA_RAW) */ PERIOD_START_TIME FECHA,
                      PERIOD_DURATION, 
                      FINS_ID,
                      TA_ID,
                      sum(PDN_CONNECT_FAILED_COLLISION +
                          PDN_CONNECT_FAILED_UNSPECIFIED +
                          PDN_CONNECTIVITY_FAILED_MME +
                          PDN_CONNECTIVITY_FAILED_SGW +
                          PDN_CONNECTIVITY_SUCCESSFUL +
                          PDN_CONNECTIVITY_FAILED +
                          DDBEARER_UEINIT_DEACT_SUCC +
                          DDBEARER_UEINIT_DEACT_FAIL +
                          DDBEARER_MMEINIT_DEACT_SUCC +
                          DDBEARER_MMEINIT_DEACT_ABNORM +
                          DDBEARER_PGWINIT_DEACT_SUCC) AS FLNS_3112A
              FROM PCOFNS_PS_SMMT_TA_RAW
              WHERE PERIOD_START_TIME BETWEEN TO_DATE(P_FINI,'dd.mm.yyyy') 
              AND to_date(P_FFIN,'dd.mm.yyyy')
              GROUP BY PERIOD_START_TIME, PERIOD_DURATION, FINS_ID, TA_ID
              ) LOOP
      pipe row (FLNS_OBJ(i.FECHA,i.PERIOD_DURATION,i.FINS_ID,i.TA_ID,i.FLNS_3112A));
    end LOOP;
  
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_FLNS_3112A',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
        
  END F_FLNS_3112A;
  
  FUNCTION F_FLNS_3172A(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED AS
  BEGIN
  
    FOR I IN (SELECT  /*+ INDEX_RS_ASC(PCOFNS_PS_SMMT_TA_RAW) */ PERIOD_START_TIME FECHA, 
                      PERIOD_DURATION, 
                      FINS_ID,
                      TA_ID,
                       sum( GW_INIT_BEARER_MOD_SUCCESS +
                            GW_INIT_BEARER_MOD_FAILURE +
                            HSS_INIT_BEARER_MOD_SUCCESS +
                            HSS_INIT_BEARER_MOD_FAILURE) AS FLNS_3172A
              FROM PCOFNS_PS_SMMT_TA_RAW
              WHERE PERIOD_START_TIME BETWEEN TO_DATE(P_FINI,'dd.mm.yyyy') 
                        AND to_date(P_FFIN,'dd.mm.yyyy')
              GROUP BY PERIOD_START_TIME, PERIOD_DURATION, FINS_ID, TA_ID
              ) LOOP
      pipe row (FLNS_OBJ(i.FECHA,i.PERIOD_DURATION,i.FINS_ID,i.TA_ID,i.FLNS_3172A));
    end LOOP;
  
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_FLNS_3172A',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
  END F_FLNS_3172A;
  
  FUNCTION F_FLNS_3102D(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED AS
  BEGIN
  
    FOR I IN (SELECT  /*+ INDEX_RS_ASC(PCOFNS_PS_MMMT_TA_RAW) */ PERIOD_START_TIME FECHA, 
                      PERIOD_DURATION, 
                      FINS_ID,
                      TA_ID,
                      SUM(INTRATAU_WO_SGW_CHANGE_SUCC +
                          INTRAMME_TAU_SGW_CHG_SUCC +
                          INTRATAU_WO_SGW_CHANGE_FAIL +
                          EPS_PERIODIC_TAU_SUCC +
                    EPS_PERIODIC_TAU_FAIL) AS FLNS_3102D
              FROM PCOFNS_PS_MMMT_TA_RAW
              WHERE PERIOD_START_TIME BETWEEN TO_DATE(P_FINI,'dd.mm.yyyy') 
                        AND to_date(P_FFIN,'dd.mm.yyyy')                       
              GROUP BY PERIOD_START_TIME, PERIOD_DURATION, FINS_ID, TA_ID
              ) LOOP
      pipe row (FLNS_OBJ(i.FECHA,i.PERIOD_DURATION,i.FINS_ID,i.TA_ID,i.FLNS_3102D));
    END LOOP;

    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_FLNS_3102D',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
  END F_FLNS_3102D;
  
  FUNCTION F_FLNS_3113A(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED AS
  BEGIN
  
    for I in (SELECT  /*+ INDEX_RS_ASC(PCOFNS_PS_MMMT_TA_RAW) */ PERIOD_START_TIME FECHA,
                      PERIOD_DURATION, 
                      FINS_ID,
                      TA_ID,
                      sum(INTERMME_TAU_WO_SGW_CHG_SUCC +
                          INTERMME_TAU_IN_FAIL +
                          INTERMME_TAU_OUT_SUCC +
                          INTERMME_TAU_OUT_FAIL) AS FLNS_3113A
              FROM PCOFNS_PS_MMMT_TA_RAW
              WHERE PERIOD_START_TIME BETWEEN TO_DATE(P_FINI,'dd.mm.yyyy') 
                        AND to_date(P_FFIN,'dd.mm.yyyy')
              GROUP BY PERIOD_START_TIME, PERIOD_DURATION, FINS_ID, TA_ID
              ) LOOP
      pipe row (FLNS_OBJ(i.FECHA,i.PERIOD_DURATION,i.FINS_ID,i.TA_ID,i.FLNS_3113A));
    end LOOP;
  
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_FLNS_3113A',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
  END F_FLNS_3113A;
  
  FUNCTION F_FLNS_3103B(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED AS
  BEGIN
  
    for I in (SELECT  /*+ INDEX_RS_ASC(PCOFNS_PS_MMMT_TA_RAW) */ PERIOD_START_TIME FECHA,
                      PERIOD_DURATION,
                      FINS_ID,
                      TA_ID,
                      sum(EPS_INTER_TAU_OG_SUCC + EPS_INTER_TAU_OG_FAIL) AS FLNS_3103B
              FROM PCOFNS_PS_MMMT_TA_RAW
              WHERE PERIOD_START_TIME BETWEEN TO_DATE(P_FINI,'dd.mm.yyyy') 
                        AND to_date(P_FFIN,'dd.mm.yyyy')
              GROUP BY PERIOD_START_TIME, PERIOD_DURATION, FINS_ID, TA_ID
              ) LOOP
      pipe row (FLNS_OBJ(i.FECHA,i.PERIOD_DURATION,i.FINS_ID,i.TA_ID,i.FLNS_3103B));
    end LOOP;
  
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_FLNS_3103B',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
  END F_FLNS_3103B;
  
  FUNCTION F_FLNS_3104B(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED AS
  BEGIN
  
    for I in (SELECT  /*+ INDEX_RS_ASC(PCOFNS_PS_MMMT_TA_RAW) */ PERIOD_START_TIME FECHA,
                      PERIOD_DURATION, 
                      FINS_ID,
                      TA_ID,
                      sum(EPS_PATH_SWITCH_X2_SUCC +
                          EPS_X2HO_SGW_CHG_SUCC +
                          EPS_PATH_SWITCH_X2_FAIL +
                          EPS_S1HO_SUCC +
                          INTRAMME_S1HO_SGW_CHG_SUCC +
                          EPS_S1HO_FAIL +
                          INTRAMME_S1HO_SGW_CHG_FAIL) AS FLNS_3104B
                FROM PCOFNS_PS_MMMT_TA_RAW
                WHERE PERIOD_START_TIME BETWEEN TO_DATE(P_FINI,'dd.mm.yyyy') 
                        AND to_date(P_FFIN,'dd.mm.yyyy')
                GROUP BY PERIOD_START_TIME, PERIOD_DURATION, FINS_ID, TA_ID
              ) LOOP
      pipe row (FLNS_OBJ(i.FECHA,i.PERIOD_DURATION,i.FINS_ID,i.TA_ID,i.FLNS_3104B));
    end LOOP;
  
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_FLNS_3104B',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
  END F_FLNS_3104B;
  
  FUNCTION F_FLNS_3114A(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED AS
  BEGIN
  
    for I in (SELECT  /*+ INDEX_RS_ASC(PCOFNS_PS_MMMT_TA_RAW) */ PERIOD_START_TIME FECHA,
                      PERIOD_DURATION, 
                      FINS_ID,
                      TA_ID,
                      sum(INTERMME_S1HO_WO_SGW_CHG_SUCC +
                          INTERMME_S1HO_IN_FAIL +
                          INTERMME_S1HO_OUT_SUCC +
                          INTERMME_S1HO_OUT_FAIL) AS FLNS_3114A
                FROM PCOFNS_PS_MMMT_TA_RAW
                WHERE PERIOD_START_TIME BETWEEN TO_DATE(P_FINI,'dd.mm.yyyy') 
                        AND to_date(P_FFIN,'dd.mm.yyyy')
                GROUP BY PERIOD_START_TIME, PERIOD_DURATION, FINS_ID, TA_ID
              ) LOOP
      pipe row (FLNS_OBJ(i.FECHA,i.PERIOD_DURATION,i.FINS_ID,i.TA_ID,i.FLNS_3114A));
    end LOOP;
  
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_FLNS_3114A',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
  END F_FLNS_3114A;
  
  FUNCTION F_FLNS_3105B(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED AS
  BEGIN
  
    for I in (SELECT  /*+ INDEX_RS_ASC(PCOFNS_PS_MMMT_TA_RAW) INDEX_RS_ASC(PCOFNS_PS_UMLM_FLEXINS_RAW)*/t1.PERIOD_START_TIME FECHA,
                      t1.PERIOD_DURATION, 
                      t1.FINS_ID,
                      t1.TA_ID,
                      SUM(EPS_TO_3G_GN_ISHO_SUCC +
                          EPS_TO_3G_GN_ISHO_FAIL +
                          EPS_TO_3G_GN_ISHO_TRGT_REJE +
                          EPS_TO_3G_GN_ISHO_ENB_CNCL -
                          SRVCC_PS_AND_CS_HANDOVER_SUCC -
                          SRVCC_PS_AND_CS_HO_3G_FAIL) AS FLNS_3105B
              FROM PCOFNS_PS_MMMT_TA_RAW t1
              INNER JOIN PCOFNS_PS_UMLM_FLEXINS_RAW t2
                  ON t1.FINS_ID = t2.FINS_ID
                  AND t1.PERIOD_START_TIME = t2.PERIOD_START_TIME
              WHERE t1.PERIOD_START_TIME BETWEEN TO_DATE(P_FINI,'dd.mm.yyyy') 
                        AND to_date(P_FFIN,'dd.mm.yyyy')
              GROUP BY t1.PERIOD_START_TIME, t1.PERIOD_DURATION, t1.FINS_ID, t1.TA_ID
              ) LOOP
      pipe row (FLNS_OBJ(i.FECHA,i.PERIOD_DURATION,i.FINS_ID,i.TA_ID,i.FLNS_3105B));
    end LOOP;
  
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_FLNS_3105B',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
  END F_FLNS_3105B;
  
  FUNCTION F_FLNS_3106A(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED AS
  BEGIN
  
    for I in (SELECT  /*+ INDEX_RS_ASC(PCOFNS_PS_MMMT_TA_RAW) */ PERIOD_START_TIME FECHA,
                      PERIOD_DURATION, 
                      FINS_ID,
                      TA_ID,
                      sum(EPS_PAGING_SUCC + EPS_PAGING_FAIL) AS FLNS_3106A
              FROM PCOFNS_PS_MMMT_TA_RAW
              WHERE PERIOD_START_TIME BETWEEN TO_DATE(P_FINI,'dd.mm.yyyy') 
                        AND to_date(P_FFIN,'dd.mm.yyyy')
              GROUP BY PERIOD_START_TIME, PERIOD_DURATION, FINS_ID, TA_ID
              ) LOOP
      pipe row (FLNS_OBJ(i.FECHA,i.PERIOD_DURATION,i.FINS_ID,i.TA_ID,i.FLNS_3106A));
    end LOOP;
  
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_FLNS_3106A',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
  END F_FLNS_3106A;
  
  FUNCTION F_FLNS_3218A(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED AS
  BEGIN
  
    for I in (SELECT  /*+ INDEX_RS_ASC(PCOFNS_PS_MMMT_TA_RAW) */ PERIOD_START_TIME FECHA,
                      PERIOD_DURATION, 
                      FINS_ID,
                      TA_ID,
                      sum(EPS_SERVICE_REQUEST_SUCC +
                      EPS_SERVICE_REQUEST_FAIL -
                      EPS_PAGING_SUCC) AS FLNS_3218A
              FROM PCOFNS_PS_MMMT_TA_RAW
              WHERE PERIOD_START_TIME BETWEEN TO_DATE(P_FINI,'dd.mm.yyyy') 
                        AND to_date(P_FFIN,'dd.mm.yyyy')
              GROUP BY PERIOD_START_TIME, PERIOD_DURATION, FINS_ID, TA_ID
              ) LOOP
      pipe row (FLNS_OBJ(i.FECHA,i.PERIOD_DURATION,i.FINS_ID,i.TA_ID,i.FLNS_3218A));
    end LOOP;
  
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_FLNS_3218A',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
  END F_FLNS_3218A;
  
  FUNCTION F_FLNS_3115A(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED AS
  BEGIN
  
    FOR I IN (SELECT  /*+ INDEX_RS_ASC(PCOFNS_PS_MMMT_TA_RAW) */ PERIOD_START_TIME FECHA, 
                      PERIOD_DURATION, 
                      FINS_ID,
                      TA_ID,
                      sum(EPS_SERVICE_REQUEST_SUCC) AS FLNS_3115A
              FROM PCOFNS_PS_MMMT_TA_RAW
              WHERE PERIOD_START_TIME BETWEEN TO_DATE(P_FINI,'dd.mm.yyyy') 
                        AND to_date(P_FFIN,'dd.mm.yyyy')
              GROUP BY PERIOD_START_TIME, PERIOD_DURATION, FINS_ID, TA_ID
              ) LOOP
      pipe row (FLNS_OBJ(i.FECHA,i.PERIOD_DURATION,i.FINS_ID,i.TA_ID,i.FLNS_3115A));
    end LOOP;
  
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_FLNS_3115A',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
  END F_FLNS_3115A;
  
  FUNCTION F_FLNS_3108B(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED AS
  BEGIN
  
    FOR I IN (SELECT  /*+ INDEX_RS_ASC(PCOFNS_PS_MMMT_TA_RAW) */ PERIOD_START_TIME FECHA, 
                      PERIOD_DURATION, 
                      FINS_ID,
                      TA_ID,
                      sum(ESR_MO_ATTEMPTS +
                          ESR_MT_ATTEMPTS +
                          ESR_MO_EMERGENCY_ATTEMPTS) AS FLNS_3108B
              FROM PCOFNS_PS_MMMT_TA_RAW
              WHERE PERIOD_START_TIME BETWEEN TO_DATE(P_FINI,'dd.mm.yyyy') 
                        AND to_date(P_FFIN,'dd.mm.yyyy')
              GROUP BY PERIOD_START_TIME, PERIOD_DURATION, FINS_ID, TA_ID
              ) LOOP
      pipe row (FLNS_OBJ(i.FECHA,i.PERIOD_DURATION,i.FINS_ID,i.TA_ID,i.FLNS_3108B));
    end LOOP;
  
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_FLNS_3108B',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
  END F_FLNS_3108B;
  
  FUNCTION F_FLNS_3109B(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB2 PIPELINED AS
  begin
 
    FOR I IN (SELECT  /*+ INDEX_RS_ASC(PCOFNS_PS_SGSM_FLEXINS_RAW) */ 
                      PERIOD_START_TIME FECHA,
                      PERIOD_DURATION,
                      FINS_ID,
                      --TA_ID,
                      sum(SGSAP_UPLINK_SUCC +
                          SGSAP_UPLINK_FAIL +
                          SGSAP_DOWNLINK_SUCC +
                          SGSAP_DOWNLINK_FAIL) AS FLNS_3109B
              FROM PCOFNS_PS_SGSM_FLEXINS_RAW
              WHERE PERIOD_START_TIME BETWEEN TO_DATE(P_FINI,'dd.mm.yyyy') 
                        AND to_date(P_FFIN,'dd.mm.yyyy')
              GROUP BY PERIOD_START_TIME, PERIOD_DURATION, FINS_ID--, TA_ID i.TA_ID
              ) LOOP
      pipe row (FLNS_OBJ2(i.FECHA,i.PERIOD_DURATION,i.FINS_ID,i.FLNS_3109B));
    end LOOP;
  
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_FLNS_3109B',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
  END F_FLNS_3109B;
  
  FUNCTION F_FLNS_3287A(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB2 PIPELINED AS
  BEGIN
    FOR i IN (SELECT  /*+ INDEX_RS_ASC(PCOFNS_PS_UMLM_FLEXINS_RAW) */
                      PERIOD_START_TIME FECHA, 
                      PERIOD_DURATION, 
                      FINS_ID,
                      --TA_ID,
                      SUM(SRVCC_CS_HO_2G_SUCC +
                          SRVCC_CS_HO_2G_FAIL +
                          SRVCC_CS_HANDOVER_SUCC +
                          SRVCC_CS_HO_3G_FAIL +
                          SRVCC_PS_AND_CS_HANDOVER_SUCC +
                          SRVCC_PS_AND_CS_HO_3G_FAIL +
                          SRVCC_EME_HO_2G_SUCC +
                          SRVCC_EME_HO_2G_FAIL +
                          SRVCC_EME_HO_3G_SUCC +
                          SRVCC_EME_HO_3G_FAIL) AS FLNS_3287A
              FROM PCOFNS_PS_UMLM_FLEXINS_RAW
              WHERE PERIOD_START_TIME BETWEEN TO_DATE(P_FINI,'dd.mm.yyyy') 
                        AND to_date(P_FFIN,'dd.mm.yyyy')
              GROUP BY PERIOD_START_TIME, PERIOD_DURATION, FINS_ID--, TA_ID i.TA_ID
              ) loop
      pipe row (FLNS_OBJ2(i.FECHA,i.PERIOD_DURATION,i.FINS_ID,i.FLNS_3287A));
    END loop;
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_FLNS_3109B',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
  END F_FLNS_3287A;
  /**
  * Para contener los resultados
  */
  FUNCTION F_CONSOLIDADO (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED AS
    v_ffin VARCHAR2(10 CHAR) := '';
  BEGIN
    IF P_FFIN IS NULL THEN
      V_FFIN  := P_FINI;
    ELSE
      V_FFIN := P_FFIN;
    END IF;
    
    for i in (SELECT FLNS_3100A_.FECHA, 
                      FLNS_3100A_.PERIOD_DURATION, 
                      FLNS_3100A_.FINS_ID, 
                      sum(FLNS_3100A_.FLNS_VALUE+FLNS_3101A_.FLNS_VALUE+FLNS_3111A_.FLNS_VALUE+FLNS_3112A_.FLNS_VALUE+
                      FLNS_3172A_.FLNS_VALUE+FLNS_3102D_.FLNS_VALUE+FLNS_3113A_.FLNS_VALUE+FLNS_3103B_.FLNS_VALUE+
                      FLNS_3104B_.FLNS_VALUE+FLNS_3114A_.FLNS_VALUE+FLNS_3105B_.FLNS_VALUE+FLNS_3106A_.FLNS_VALUE+
                      FLNS_3218A_.FLNS_VALUE+FLNS_3115A_.FLNS_VALUE+FLNS_3108B_.FLNS_VALUE+FLNS_3109B_.FLNS_VALUE) FLNS_VALUE
              FROM 
                  (SELECT /*materialize */ FECHA, 
                          PERIOD_DURATION, 
                          FINS_ID,
                          TA_ID,
                          NVL(FLNS_VALUE,0) FLNS_VALUE
                  FROM TABLE(F_FLNS_3100A(P_FINI,V_FFIN))) FLNS_3100A_ 
                  inner join
                  (SELECT /*materialize */ FECHA, 
                        PERIOD_DURATION, 
                        FINS_ID,
                        TA_ID,
                        NVL(FLNS_VALUE,0) FLNS_VALUE
                  FROM TABLE(F_FLNS_3101A(P_FINI,V_FFIN)))FLNS_3101A_ 
                          ON FLNS_3100A_.FINS_ID = FLNS_3101A_.FINS_ID 
                          AND FLNS_3100A_.FECHA = FLNS_3101A_.FECHA
                          AND FLNS_3100A_.TA_ID = FLNS_3101A_.TA_ID
                  inner join
                  (SELECT /*materialize */ FECHA,
                          PERIOD_DURATION, 
                          FINS_ID,
                          TA_ID,
                          NVL(FLNS_VALUE,0) FLNS_VALUE
                  FROM TABLE(F_FLNS_3111A(P_FINI,V_FFIN))) FLNS_3111A_
                    ON FLNS_3111A_.FINS_ID = FLNS_3101A_.FINS_ID 
                    AND FLNS_3111A_.FECHA = FLNS_3101A_.FECHA
                    AND FLNS_3111A_.TA_ID = FLNS_3101A_.TA_ID
                  INNER JOIN
                  (SELECT /*materialize */ FECHA, 
                          PERIOD_DURATION, 
                          FINS_ID,
                          TA_ID,
                          NVL(FLNS_VALUE,0) FLNS_VALUE
                  FROM TABLE(F_FLNS_3112A(P_FINI,V_FFIN)))FLNS_3112A_
                    ON FLNS_3111A_.FINS_ID = FLNS_3112A_.FINS_ID 
                    AND FLNS_3111A_.FECHA = FLNS_3112A_.FECHA
                    AND FLNS_3111A_.TA_ID = FLNS_3112A_.TA_ID
                  INNER JOIN
                  (SELECT /*materialize */ FECHA, 
                          PERIOD_DURATION, 
                          FINS_ID,
                          TA_ID,
                          NVL(FLNS_VALUE,0) FLNS_VALUE
                  FROM TABLE(F_FLNS_3172A(P_FINI,V_FFIN)))FLNS_3172A_
                    ON FLNS_3172A_.FINS_ID = FLNS_3112A_.FINS_ID 
                    AND FLNS_3172A_.FECHA = FLNS_3112A_.FECHA
                    AND FLNS_3172A_.TA_ID = FLNS_3112A_.TA_ID
                  INNER JOIN
                  (SELECT /*materialize */ FECHA, 
                          PERIOD_DURATION, 
                          FINS_ID,
                          TA_ID,
                          NVL(FLNS_VALUE,0) FLNS_VALUE
                  FROM TABLE(F_FLNS_3102D(P_FINI,V_FFIN)))FLNS_3102D_
                    ON FLNS_3172A_.FINS_ID = FLNS_3102D_.FINS_ID 
                    AND FLNS_3172A_.FECHA = FLNS_3102D_.FECHA
                    AND FLNS_3172A_.TA_ID = FLNS_3102D_.TA_ID
                  INNER JOIN
                  (SELECT /*materialize */ FECHA, 
                          PERIOD_DURATION, 
                          FINS_ID,
                          TA_ID,
                          NVL(FLNS_VALUE,0) FLNS_VALUE
                  FROM TABLE(F_FLNS_3113A(P_FINI,V_FFIN)))FLNS_3113A_
                    ON FLNS_3113A_.FINS_ID = FLNS_3102D_.FINS_ID 
                    AND FLNS_3113A_.FECHA = FLNS_3102D_.FECHA
                    AND FLNS_3113A_.TA_ID = FLNS_3102D_.TA_ID
                  INNER JOIN
                  (SELECT /*materialize */ FECHA, 
                          PERIOD_DURATION, 
                          FINS_ID,
                          TA_ID,
                          NVL(FLNS_VALUE,0) FLNS_VALUE
                  FROM TABLE(F_FLNS_3103B(P_FINI,V_FFIN)))FLNS_3103B_
                    ON FLNS_3113A_.FINS_ID = FLNS_3103B_.FINS_ID 
                    AND FLNS_3113A_.FECHA = FLNS_3103B_.FECHA
                    AND FLNS_3113A_.TA_ID = FLNS_3103B_.TA_ID
                  INNER JOIN
                  (SELECT /*materialize */ FECHA, 
                          PERIOD_DURATION, 
                          FINS_ID,
                          TA_ID,
                          NVL(FLNS_VALUE,0) FLNS_VALUE
                  FROM TABLE(F_FLNS_3104B(P_FINI,V_FFIN)))FLNS_3104B_
                    ON FLNS_3104B_.FINS_ID = FLNS_3103B_.FINS_ID 
                    AND FLNS_3104B_.FECHA = FLNS_3103B_.FECHA
                    AND FLNS_3104B_.TA_ID = FLNS_3103B_.TA_ID
                  INNER JOIN
                  (SELECT /*materialize */ FECHA, 
                          PERIOD_DURATION, 
                          FINS_ID,
                          TA_ID,
                          NVL(FLNS_VALUE,0) FLNS_VALUE
                  FROM TABLE(F_FLNS_3114A(P_FINI,V_FFIN)))FLNS_3114A_
                    ON FLNS_3104B_.FINS_ID = FLNS_3114A_.FINS_ID 
                    AND FLNS_3104B_.FECHA = FLNS_3114A_.FECHA
                    AND FLNS_3104B_.TA_ID = FLNS_3114A_.TA_ID
                  INNER JOIN
                  (SELECT /*materialize */ FECHA, 
                          PERIOD_DURATION, 
                          FINS_ID,
                          TA_ID,
                          NVL(FLNS_VALUE,0) FLNS_VALUE
                  FROM TABLE(F_FLNS_3105B(P_FINI,V_FFIN)))FLNS_3105B_
                  ON FLNS_3105B_.FINS_ID = FLNS_3114A_.FINS_ID 
                  AND FLNS_3105B_.FECHA = FLNS_3114A_.FECHA
                  AND FLNS_3105B_.TA_ID = FLNS_3114A_.TA_ID
                  INNER JOIN
                  (SELECT /*materialize */ FECHA, 
                          PERIOD_DURATION, 
                          FINS_ID,
                          TA_ID,
                          NVL(FLNS_VALUE,0) FLNS_VALUE
                  FROM TABLE(F_FLNS_3106A(P_FINI,V_FFIN)))FLNS_3106A_
                    ON FLNS_3105B_.FINS_ID = FLNS_3106A_.FINS_ID 
                    AND FLNS_3105B_.FECHA = FLNS_3106A_.FECHA
                    AND FLNS_3105B_.TA_ID = FLNS_3106A_.TA_ID
                  INNER JOIN
                  (SELECT /*materialize */ FECHA, 
                          PERIOD_DURATION, 
                          FINS_ID,
                          TA_ID,
                          NVL(FLNS_VALUE,0) FLNS_VALUE
                  FROM TABLE(F_FLNS_3218A(P_FINI,V_FFIN)))FLNS_3218A_
                    ON FLNS_3218A_.FINS_ID = FLNS_3106A_.FINS_ID 
                    AND FLNS_3218A_.FECHA = FLNS_3106A_.FECHA
                    --AND FLNS_3218A_.TA_ID = FLNS_3106A_.TA_ID
                  INNER JOIN
                  (SELECT /*materialize */ FECHA, 
                          PERIOD_DURATION, 
                          FINS_ID,
                          TA_ID,
                          NVL(FLNS_VALUE,0) FLNS_VALUE
                  FROM TABLE(F_FLNS_3115A(P_FINI,V_FFIN)))FLNS_3115A_
                    ON FLNS_3218A_.FINS_ID = FLNS_3115A_.FINS_ID 
                    AND FLNS_3218A_.FECHA = FLNS_3115A_.FECHA
                    AND FLNS_3218A_.TA_ID = FLNS_3115A_.TA_ID
                  INNER JOIN
                  (SELECT /*materialize */ FECHA, 
                          PERIOD_DURATION, 
                          FINS_ID,
                          TA_ID,
                          NVL(FLNS_VALUE,0) FLNS_VALUE
                  FROM TABLE(F_FLNS_3108B(P_FINI,V_FFIN)))FLNS_3108B_
                    ON FLNS_3108B_.FINS_ID = FLNS_3115A_.FINS_ID 
                    AND FLNS_3108B_.FECHA = FLNS_3115A_.FECHA
                    AND FLNS_3108B_.TA_ID = FLNS_3115A_.TA_ID
                  INNER JOIN
                  (SELECT /*materialize */ FECHA, 
                          PERIOD_DURATION, 
                          FINS_ID,
                          --TA_ID,
                          NVL(FLNS_VALUE,0) FLNS_VALUE
                  FROM TABLE(F_FLNS_3109B(P_FINI,V_FFIN)))FLNS_3109B_
                    ON FLNS_3108B_.FINS_ID = FLNS_3109B_.FINS_ID 
                    AND FLNS_3108B_.FECHA = FLNS_3109B_.FECHA
                    --AND FLNS_3108B_.TA_ID = FLNS_3109B_.TA_ID
                  INNER JOIN
                  (SELECT /*materialize */ FECHA, 
                          PERIOD_DURATION, 
                          FINS_ID, 
                          NVL(FLNS_VALUE,0) FLNS_VALUE
                  FROM TABLE(F_FLNS_3287A(P_FINI,V_FFIN)))FLNS_3287A_
                    ON FLNS_3287A_.FINS_ID = FLNS_3109B_.FINS_ID 
                    AND FLNS_3287A_.FECHA = FLNS_3109B_.FECHA
              --
              GROUP BY FLNS_3100A_.FECHA, 
                      FLNS_3100A_.PERIOD_DURATION, 
                      FLNS_3100A_.FINS_ID
                      --FLNS_3100A_.TA_ID
                      ) loop
      pipe row (FLNS_OBJ(i.FECHA,i.PERIOD_DURATION,i.FINS_ID,null,i.FLNS_VALUE));
    END loop;
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_CONSOLIDADO',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
  end F_CONSOLIDADO;
  
END PKG_LTE_CORE;
/
