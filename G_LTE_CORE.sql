CREATE OR REPLACE PACKAGE G_LTE_CORE AS 

  /**
  * Author: Carrasco Marcelo
  * Date: 11/02/2016
  * Comment: Paquete para la importacion y visualizaci?n de datos de LTE
  */
  p_limit_in CONSTANT PLS_INTEGER := 100;
  p_source1_mobility_management  CONSTANT VARCHAR2(100)  := 'PCOFNS_PS_MMMT_TA_RAW@OSSRC6.WORLD';
  P_target_mobility_management   CONSTANT VARCHAR2(100)  := 'PCOFNS_PS_MMMT_TA_RAW';
  p_source1_session_management   CONSTANT VARCHAR2(100)  := 'PCOFNS_PS_SMMT_TA_RAW@OSSRC6.WORLD';
  P_target_session_management    CONSTANT VARCHAR2(100)  := 'PCOFNS_PS_SMMT_TA_RAW';
  p_source1_mmdu_user_level      CONSTANT VARCHAR2(100)  := 'PCOFNS_PS_MULM_MMDU_RAW@OSSRC6.WORLD';
  P_target_mmdu_user_level       CONSTANT VARCHAR2(100)  := 'PCOFNS_PS_MULM_MMDU_RAW';
  p_source1_user_mme_level       CONSTANT VARCHAR2(100)  := 'PCOFNS_PS_UMLM_FLEXINS_RAW@OSSRC6.WORLD';
  P_target_user_mme_level        CONSTANT VARCHAR2(100)  := 'PCOFNS_PS_UMLM_FLEXINS_RAW';
  p_source1_SGsAP                CONSTANT VARCHAR2(100)  := 'PCOFNS_PS_SGSM_FLEXINS_RAW@OSSRC6.WORLD';
  P_target_SGsAP                 CONSTANT VARCHAR2(100)  := 'PCOFNS_PS_SGSM_FLEXINS_RAW';
  p_source1_computer_unit_load   CONSTANT VARCHAR2(100)  := 'PCOFNS_PS_ULOAD_UNIT1_RAW@OSSRC6.WORLD';
  P_target_computer_unit_load    CONSTANT VARCHAR2(100)  := 'PCOFNS_PS_ULOAD_UNIT1_RAW';
  p_source1_network_element_user CONSTANT VARCHAR2(100)  := 'PCOFNS_PS_NEUM_FLEXINS_RAW@OSSRC6.WORLD';
  P_target_network_element_user  CONSTANT VARCHAR2(100)  := 'PCOFNS_PS_NEUM_FLEXINS_RAW';
  
  /**
  * Procedimientos para importar datos desde OSSRC
  */
  PROCEDURE p_imp_LTE_Mobility_Management(p_source IN VARCHAR2, P_target  IN VARCHAR2);
  PROCEDURE p_imp_LTE_Session_Management(p_sourcE IN VARCHAR2,P_target   IN VARCHAR2);
  PROCEDURE p_imp_LTE_MMDU_User_Level(p_source IN VARCHAR2,P_target   IN VARCHAR2);
  PROCEDURE p_imp_LTE_USER_MME_LEVEL(p_source IN VARCHAR2,P_target IN VARCHAR2);
  PROCEDURE p_imp_LTE_SGsAP(p_source IN VARCHAR2,P_target IN VARCHAR2);
  PROCEDURE p_imp_LTE_Computer_Unit_Load(p_source IN VARCHAR2,P_target IN VARCHAR2);
  PROCEDURE P_IMP_LTE_NETWORK_ELEMENTUSER(P_SOURCE IN VARCHAR2,P_TARGET IN VARCHAR2);
  PROCEDURE P_CALL_IMPORT_PROCEDURES;
  /**
  *
  */ 
  --(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) 
  FUNCTION F_MMMT RETURN FLNS_MMMT_TAB PIPELINED;
  --(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL)
  FUNCTION F_SMMT  RETURN FLNS_SMMT_TAB PIPELINED;
  --(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL)
  FUNCTION F_UMLM_MMMT  RETURN FLNS_UMLM_MMMT_TAB PIPELINED;
  --(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL)
  FUNCTION FLNS_SGSM  RETURN FLNS_SGSM_TAB PIPELINED;
 -- (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL)
  FUNCTION F_TPS  RETURN FLNS_TAB PIPELINED;
  
  PROCEDURE P_TPS (RESULT_SET OUT SYS_REFCURSOR);
  --
  -- CON FECHA
  --
  --
  FUNCTION F_MMMT2(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL)  RETURN FLNS_MMMT_TAB PIPELINED;
  --
  FUNCTION F_SMMT2 (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_SMMT_TAB PIPELINED;
  --
  FUNCTION F_UMLM_MMMT2 (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_UMLM_MMMT_TAB PIPELINED;
  --
  FUNCTION FLNS_SGSM2 (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_SGSM_TAB PIPELINED;
 -- 
  FUNCTION F_LTE_CORE_TPS_HOUR (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED;
  
  FUNCTION F_LTE_CORE_TPS_DAY (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED;
  
  PROCEDURE P_TPS2 (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL,RESULT_SET OUT SYS_REFCURSOR);
END G_LTE_CORE;
/


CREATE OR REPLACE PACKAGE BODY G_LTE_CORE AS

  /**
  * Procedimientos para importar datos desde OSSRC
  */
  PROCEDURE P_IMP_LTE_MOBILITY_MANAGEMENT(P_SOURCE IN VARCHAR2, P_TARGET  IN VARCHAR2) IS

  sql_stmt VARCHAR2(32767);
  TYPE cur_typ IS REF CURSOR;
  cur CUR_TYP;
  -- LOG --
  l_errors number;
  l_errno    number;
  l_msg    varchar2(4000);
  l_idx    number;
  -- END LOG --

  TYPE t_array_tab IS TABLE OF PCOFNS_PS_MMMT_TA_RAW%ROWTYPE;
  t_array_col t_array_tab;

  BEGIN
    sql_stmt := 'SELECT T1.FINS_ID,
                        T1.MCC_ID,
                        T1.MNC_ID,
                        T1.TA_ID,
                        T1.PERIOD_START_TIME,
                        T1.PERIOD_DURATION,
                        T1.EPS_ATTACH_SUCC,
                        T1.EPS_ATTACH_FAIL,
                        T1.EPS_DETACH,
                        T1.EPS_PERIODIC_TAU_SUCC,
                        T1.EPS_PERIODIC_TAU_FAIL,
                        T1.EPS_TAU_SUCC,
                        T1.EPS_TAU_FAIL,
                        T1.EPS_SERVICE_REQUEST_SUCC,
                        T1.EPS_SERVICE_REQUEST_FAIL,
                        T1.EPS_PATH_SWITCH_X2_SUCC,
                        T1.EPS_PATH_SWITCH_X2_FAIL,
                        T1.EPS_PAGING_ATTEMPTS,
                        T1.EPS_PAGING_SUCC,
                        T1.EPS_PAGING_FAIL,
                        T1.EPS_TO_3G_GN_ISHO_SUCC,
                        T1.EPS_TO_3G_GN_ISHO_TRGT_REJE,
                        T1.EPS_TO_3G_GN_ISHO_PREP_FAIL,
                        T1.EPS_TO_3G_GN_ISHO_ENB_CNCL,
                        T1.EPS_TO_3G_GN_ISHO_FAIL,
                        T1.EPS_S1HO_SUCC,
                        T1.EPS_S1HO_FAIL,
                        T1.EPS_S1HO_DUE_TO_MME_FAIL,
                        T1.EPS_S1HO_ENB_ERROR_FAIL,
                        T1.EPS_S1HO_SGW_ERROR_FAIL,
                        T1.EPS_S1HO_COLLISION_FAIL,
                        T1.EPS_S1HO_UNSPECIFIED_RE_FAIL,
                        T1.EPS_S1HO_UNKNOWN_TARGET_FAIL,
                        T1.EPS_S1HO_IN_TARGET_SYST_FAIL,
                        T1.EPS_S1HO_HO_CANCELLED_FAIL,
                        T1.EPS_S1HO_INTERACTION_WI_FAIL,
                        T1.EPS_S1HO_TARGET_ALLOW_FAIL,
                        T1.EPS_S1HO_CELL_NOT_AVAIL_FAIL,
                        T1.EPS_S1HO_NO_RADIO_RESOU_FAIL,
                        T1.EPS_S1HO_ALG_NOT_SUPP_FAIL,
                        T1.EPS_S1HO_UNSPEC_CAUSE_FAIL,
                        T1.EPS_CMB_INTRA_TAU_SUCC,
                        T1.EPS_CMB_INTRA_TAU_FAIL,
                        T1.EPS_CMB_INTRA_TAU_IMSI_ATT_SUC,
                        T1.EPS_CMB_INTRA_TAU_IMSI_ATT_FAI,
                        T1.EPS_CMB_INTER_TAU_SUCC,
                        T1.EPS_CMB_INTER_TAU_FAIL,
                        T1.EPS_INTER_TAU_OG_SUCC,
                        T1.EPS_INTER_TAU_OG_FAIL,
                        T1.INTRATAU_WO_SGW_CHANGE_SUCC,
                        T1.INTRATAU_WO_SGW_CHANGE_FAIL,
                        T1.EPS_COMBINED_ATTACH_SUCC,
                        T1.EPS_CMBND_ATTACH_EPS_SUCC,
                        T1.EPS_CMBND_ATTACH_EPS_FAIL,
                        T1.EPS_CMBND_ATTACH_FAIL,
                        T1.ESR_MO_ATTEMPTS,
                        T1.ESR_MT_ATTEMPTS,
                        T1.ESR_MO_EMERGENCY_ATTEMPTS,
                        T1.EPS_ATTACH_SUCC_UE_APN,
                        T1.EPS_ATTACH_SUCC_UE_APN_OVERRDN,
                        T1.EPS_ATTACH_FAIL_ESM_INFO_REQ,
                        T1.INTRAMME_S1HO_SGW_CHG_FAIL,
                        T1.INTRAMME_S1HO_SGW_CHG_SUCC,
                        T1.EPS_X2HO_SGW_CHG_SUCC,
                        T1.INTRAMME_TAU_SGW_CHG_SUCC,
                        T1.MME_OFFLOAD_SUCCESSFUL,
                        T1.MME_OFFLOAD_SUCCESS_ABNORMAL,
                        T1.MME_OFFLOAD_FAILED,
                        T1.MME_OFFLOAD_FAILED_ENB_ERROR,
                        T1.MME_OFFLOAD_FAILED_COLLISIONS,
                        T1.INTERMME_TAU_WO_SGW_CHG_SUCC,
                        T1.INTERMME_TAU_OUT_SUCC,
                        T1.INTERMME_S1HO_WO_SGW_CHG_SUCC,
                        T1.INTERMME_S1HO_OUT_SUCC,
                        T1.INTERMME_TAU_IN_FAIL,
                        T1.INTERMME_TAU_OUT_FAIL,
                        T1.INTERMME_S1HO_IN_FAIL,
                        T1.INTERMME_S1HO_OUT_FAIL,
                        T1.ATTACH_IN_WITH_MME_CHG_SUCC,
                        T1.ATTACH_IN_WITH_MME_CHG_FAIL,
                        T1.EPS_ATTACH_DUE_TO_MME_FAIL,
                        T1.EPS_TAU_DUE_TO_MME_FAIL,
                        T1.EPS_SERVICE_REQUEST_MME_FAIL,
                        T1.EPS_ATTACH_NAS_03_FAIL,
                        T1.EPS_ATTACH_NAS_06_FAIL,
                        T1.EPS_ATTACH_NAS_07_FAIL,
                        T1.EPS_ATTACH_NAS_08_FAIL,
                        T1.EPS_TAU_NAS_03_FAIL,
                        T1.EPS_TAU_NAS_06_FAIL,
                        T1.EPS_TAU_NAS_07_FAIL,
                        T1.EPS_TAU_NAS_08_FAIL,
                        T1.EPS_PAGING_1ST_ATTEMPT_SUCC
                FROM ' || p_source || ' T1
                LEFT OUTER JOIN ' || p_target || ' T3
                  ON T1.FINS_ID = T3.FINS_ID
                  AND T1.PERIOD_START_TIME = T3.PERIOD_START_TIME
                WHERE T1.PERIOD_START_TIME BETWEEN (SYSDATE - 14) AND SYSDATE
                AND T3.FINS_ID IS NULL
                AND T3.PERIOD_START_TIME IS NULL'; --
  
    OPEN cur FOR sql_stmt;
    LOOP
      FETCH cur BULK COLLECT INTO t_array_col LIMIT p_limit_in;
      BEGIN
        FORALL i IN 1 .. t_array_col.COUNT SAVE EXCEPTIONS
          INSERT INTO PCOFNS_PS_MMMT_TA_RAW VALUES t_array_col (i);
        EXCEPTION
          WHEN OTHERS THEN
            -- Capture exceptions to perform operations DML
            l_errors := sql%bulk_exceptions.count;
            for i in 1 .. l_errors
            loop
                l_errno := sql%bulk_exceptions(i).error_code;
                l_msg   := sqlerrm(-l_errno);
                L_IDX   := sql%BULK_EXCEPTIONS(I).ERROR_INDEX;
                
                PKG_ERROR_LOG_NEW.P_LOG_ERROR('P_IMP_LTE_MOBILITY_MANAGEMENT',
                                              L_ERRNO,
                                              L_MSG,
                                              'FINS_ID'||'->'||TO_CHAR(T_ARRAY_COL(L_IDX).FINS_ID)||' '||
                                              'PERIOD_START_TIME'||'->'||T_ARRAY_COL(L_IDX).PERIOD_START_TIME||' '||
                                              'MCC_ID ->'||T_ARRAY_COL(L_IDX).MCC_ID||' '||
                                              'MNC_ID ->'||T_ARRAY_COL(L_IDX).MNC_ID||' '||
                                              'TA_ID ->'||T_ARRAY_COL(L_IDX).TA_ID);
            end loop;
        -- end --
      END;
      EXIT WHEN cur%NOTFOUND;
    END LOOP;
    COMMIT;
    CLOSE cur;
  EXCEPTION
    when OTHERS then
      --DBMS_OUTPUT.PUT_LINE(SQLERRM);
      PKG_ERROR_LOG_NEW.P_LOG_ERROR('P_IMP_LTE_MOBILITY_MANAGEMENT',SQLCODE,SQLERRM,DBMS_UTILITY.format_error_backtrace ); --'P_SOURCE -> '||P_SOURCE||' P_TARGET -> '||P_TARGET);
  
  END P_IMP_LTE_MOBILITY_MANAGEMENT;

  procedure P_IMP_LTE_SESSION_MANAGEMENT(P_SOURCE in varchar2,P_TARGET   in varchar2) is
    SQL_STMT VARCHAR2(4000);
    TYPE CUR_TYP IS REF CURSOR;
    CUR CUR_TYP;
  
    DML_ERRORS EXCEPTION;
    PRAGMA EXCEPTION_INIT(DML_ERRORS, -24381);
    -- LOG --
    L_ERRORS NUMBER;
    L_ERRNO    NUMBER;
    L_MSG    VARCHAR2(4000);
    L_IDX    NUMBER;
    -- END LOG --
  
    TYPE T_ARRAY_TAB IS TABLE OF PCOFNS_PS_SMMT_TA_RAW%ROWTYPE;
    T_ARRAY_COL T_ARRAY_TAB;
  BEGIN
    sql_stmt := 'SELECT T1.FINS_ID,
                        T1.MCC_ID,
                        T1.MNC_ID,
                        T1.TA_ID,
                        T1.PERIOD_START_TIME,
                        T1.PERIOD_DURATION,
                        T1.EPS_DEF_BEARER_ACT_SUCC,
                        T1.EPS_DEF_BEARER_ACT_FAIL,
                        T1.EPS_DEF_BEARER_DEACT,
                        T1.GW_INIT_BEARER_MOD_SUCCESS,
                        T1.GW_INIT_BEARER_MOD_FAILURE,
                        T1.GW_INIT_BEARER_MOD_FAIL_UE,
                        T1.GW_INIT_BEARER_MOD_FAIL_ENB,
                        T1.GW_INIT_BEARER_MOD_FAIL_SGW,
                        T1.GW_INIT_BEARER_MOD_FAIL_MME,
                        T1.HSS_INIT_BEARER_MOD_SUCCESS,
                        T1.HSS_INIT_BEARER_MOD_FAILURE,
                        T1.HSS_INIT_BEARER_MOD_FAIL_UE,
                        T1.HSS_INIT_BEARER_MOD_FAIL_ENB,
                        T1.HSS_INIT_BEARER_MOD_FAIL_SGW,
                        T1.PDN_CONNECTIVITY_SUCCESSFUL,
                        T1.PDN_CONNECTIVITY_FAILED,
                        T1.PDN_CONNECTIVITY_FAILED_MME,
                        T1.PDN_CONNECTIVITY_FAILED_SGW,
                        T1.PDN_CONNECTIVITY_FAILED_UE,
                        T1.PDN_CONNECTIVITY_FAILED_ENB,
                        T1.PDN_CONNECT_FAILED_COLLISION,
                        T1.PDN_CONNECT_FAILED_UNSPECIFIED,
                        T1.DDBEARER_HSSINIT_MOD_SUCC,
                        T1.DDBEARER_HSSINIT_MOD_FAIL,
                        T1.DDBEARER_PGWINIT_ACT_SUCC,
                        T1.DDBEARER_PGWINIT_ACT_FAIL,
                        T1.DDBEARER_PGWINIT_MOD_SUCC,
                        T1.DDBEARER_PGWINIT_MOD_FAIL,
                        T1.DDBEARER_PGWINIT_DEACT_SUCC,
                        T1.DDBEARER_MMEINIT_DEACT_SUCC,
                        T1.DDBEARER_MMEINIT_DEACT_ABNORM,
                        T1.DDBEARER_UEINIT_ACT_SUCC,
                        T1.DDBEARER_UEINIT_ACT_FAIL,
                        T1.DDBEARER_UEINIT_MOD_SUCC,
                        T1.DDBEARER_UEINIT_MOD_FAIL,
                        T1.DDBEARER_UEINIT_DEACT_SUCC,
                        T1.DDBEARER_UEINIT_DEACT_FAIL,
                        T1.EPS_DEF_BEARER_ACT_MME_FAIL,
                        T1.EPS_DDBEARER_REQ_BY_MME,
                        T1.EPS_DDBEARE_CONF_BY_UE,
                        T1.EPS_DFBEARER_S5_S8_REQ_BY_MME,
                        T1.EPS_DFBEARER_S5_S8_CONF_BY_SGW,
                        T1.EPS_DDBEARER_S5_S8_REQ_BY_MME,
                        T1.EPS_DDBEARER_S5_S8_CONF_BY_SGW,
                        T1.EMCALL_PDN_CONN_SUCC,
                        T1.EMCALL_PDN_CONN_FAIL,
                        T1.EMCALL_DED_BEARER_ACT_SUCC,
                        T1.EMCALL_DED_BEARER_ACT_FAIL
                FROM ' || p_source || ' T1
                LEFT OUTER JOIN ' || p_target || ' T3
                    ON T1.FINS_ID = T3.FINS_ID
                    AND T1.PERIOD_START_TIME = T3.PERIOD_START_TIME
                WHERE T1.PERIOD_START_TIME BETWEEN (SYSDATE - 14) AND SYSDATE
                AND T3.FINS_ID IS NULL
                AND T3.PERIOD_START_TIME IS NULL';

    OPEN cur FOR sql_stmt;
    LOOP
      FETCH cur BULK COLLECT INTO t_array_col LIMIT p_limit_in;
      BEGIN
        FORALL i IN 1 .. t_array_col.COUNT SAVE EXCEPTIONS
          INSERT INTO PCOFNS_PS_SMMT_TA_RAW VALUES t_array_col (i);
        EXCEPTION
          WHEN OTHERS THEN
            -- Capture exceptions to perform operations DML
            l_errors := sql%bulk_exceptions.count;
            for i in 1 .. l_errors
            loop
                l_errno := sql%bulk_exceptions(i).error_code;
                l_msg   := sqlerrm(-l_errno);
                L_IDX   := SQL%BULK_EXCEPTIONS(I).ERROR_INDEX;
                
                PKG_ERROR_LOG_NEW.P_LOG_ERROR('P_IMP_LTE_SESSION_MANAGEMENT',
                                              L_ERRNO,
                                              L_MSG,
                                              'FINS_ID'||'->'||TO_CHAR(T_ARRAY_COL(L_IDX).FINS_ID)||' '||
                                              'PERIOD_START_TIME'||'->'||T_ARRAY_COL(L_IDX).PERIOD_START_TIME||' '||
                                              'MCC_ID ->'||T_ARRAY_COL(L_IDX).MCC_ID||' '||
                                              'MNC_ID ->'||T_ARRAY_COL(L_IDX).MNC_ID||' '||
                                              'TA_ID ->'||T_ARRAY_COL(L_IDX).TA_ID);

            end loop;
        -- end --
      END;
      EXIT WHEN cur%NOTFOUND;
    END LOOP;
    COMMIT;
    CLOSE cur;
  EXCEPTION
    WHEN OTHERS THEN
      --DBMS_OUTPUT.PUT_LINE(SQLERRM);
      PKG_ERROR_LOG_NEW.P_LOG_ERROR('P_IMP_LTE_SESSION_MANAGEMENT',SQLCODE,SQLERRM,'P_SOURCE -> '||P_SOURCE||' P_TARGET -> '||P_TARGET);
  END P_IMP_LTE_SESSION_MANAGEMENT;

  PROCEDURE P_IMP_LTE_MMDU_USER_LEVEL(P_SOURCE IN VARCHAR2,P_TARGET   IN VARCHAR2) IS
    sql_stmt VARCHAR2(4000);
    TYPE cur_typ IS REF CURSOR;
    cur CUR_TYP;

    -- LOG --
    l_errors number;
    l_errno    number;
    l_msg    varchar2(4000);
    l_idx    number;
    -- END LOG --
  
    TYPE T_ARRAY_TAB IS TABLE OF PCOFNS_PS_MULM_MMDU_RAW%ROWTYPE;
    t_array_col t_array_tab;
  BEGIN
    sql_stmt := 'SELECT T1.FINS_ID,
                        T1.MMDU_ID,
                        T1.PERIOD_START_TIME,
                        T1.PERIOD_DURATION,
                        T1.EPS_MMDU_EMM_REG_MIN,
                        T1.EPS_MMDU_EMM_REG_MAX,
                        T1.EPS_MMDU_EMM_REG_SUM,
                        T1.EPS_MMDU_EMM_REG_DENOM,
                        T1.EPS_MMDU_EMM_DEREG_MIN,
                        T1.EPS_MMDU_EMM_DEREG_MAX,
                        T1.EPS_MMDU_EMM_DEREG_SUM,
                        T1.MMDU_EMM_DEREG_DENOM,
                        T1.EPS_BEARER_ACTIVE_NBR_MAX,
                        T1.EPS_BEARER_DEDICATED_NBR_MAX,
                        T1.EPS_BEARER_DEDICATED_NBR_SUM,
                        T1.EPS_BEARER_DEDICATED_NBR_DEN,
                        T1.EPS_BEARER_DEFAULT_NBR_MAX,
                        T1.EPS_BEARER_DEFAULT_NBR_SUM,
                        T1.EPS_BEARER_DEFAULT_NBR_DEN
                FROM ' || p_source || ' T1
                LEFT OUTER JOIN ' || p_target || ' T3
                   ON T1.FINS_ID = T3.FINS_ID
                   AND T1.PERIOD_START_TIME = T3.PERIOD_START_TIME
                WHERE T1.PERIOD_START_TIME BETWEEN (SYSDATE - 14) AND SYSDATE
                AND T3.FINS_ID IS NULL
                AND T3.PERIOD_START_TIME IS NULL' ;

    OPEN cur FOR sql_stmt;
    LOOP
      FETCH cur BULK COLLECT INTO t_array_col LIMIT p_limit_in;
      BEGIN
        FORALL i IN 1 .. t_array_col.COUNT SAVE EXCEPTIONS
          INSERT INTO PCOFNS_PS_MULM_MMDU_RAW VALUES t_array_col (i);
        EXCEPTION
          WHEN OTHERS THEN
            -- Capture exceptions to perform operations DML
            l_errors := sql%bulk_exceptions.count;
            for i in 1 .. l_errors
            loop
                l_errno := sql%bulk_exceptions(i).error_code;
                l_msg   := sqlerrm(-l_errno);
                L_IDX   := SQL%BULK_EXCEPTIONS(I).ERROR_INDEX;
                
                PKG_ERROR_LOG_NEW.P_LOG_ERROR('P_IMP_LTE_MMDU_USER_LEVEL',
                                              L_ERRNO,
                                              L_MSG,
                                              'FINS_ID'||'->'||TO_CHAR(T_ARRAY_COL(L_IDX).FINS_ID)||' '||
                                              'PERIOD_START_TIME'||'->'||T_ARRAY_COL(L_IDX).PERIOD_START_TIME||' '||
                                              'MCC_ID ->'||T_ARRAY_COL(L_IDX).MMDU_ID);
            end loop;
        -- end --
      END;
      EXIT WHEN cur%NOTFOUND;
    END LOOP;
    COMMIT;
    CLOSE cur;
    EXCEPTION
      WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE(SQLERRM);
        PKG_ERROR_LOG_NEW.P_LOG_ERROR('P_IMP_LTE_MMDU_USER_LEVEL',SQLCODE,SQLERRM,'P_SOURCE -> '||P_SOURCE||' P_TARGET -> '||P_TARGET);
  END P_IMP_LTE_MMDU_USER_LEVEL;
  
  PROCEDURE P_IMP_LTE_USER_MME_LEVEL(p_source IN VARCHAR2,P_target IN VARCHAR2) IS
    sql_stmt VARCHAR2(4000);
    TYPE cur_typ IS REF CURSOR;
    cur CUR_TYP;
    -- LOG --
    l_errors number;
    l_errno    number;
    l_msg    varchar2(4000);
    l_idx    number;
    -- END LOG --
    
    TYPE T_ARRAY_TAB IS TABLE OF PCOFNS_PS_UMLM_FLEXINS_RAW%ROWTYPE;
    t_array_col t_array_tab;
  BEGIN
    sql_stmt := 'SELECT T1.FINS_ID,
                        T1.PERIOD_START_TIME,
                        T1.PERIOD_DURATION,
                        T1.EPS_ECM_CONN_MIN,
                        T1.EPS_ECM_CONN_MAX,
                        T1.EPS_ECM_IDLE_MIN,
                        T1.EPS_ECM_IDLE_MAX,
                        T1.EPS_EMM_REG_MIN,
                        T1.EPS_EMM_REG_MAX,
                        T1.EPS_EMM_DEREG_MIN,
                        T1.EPS_EMM_DEREG_MAX,
                        T1.EPS_ECM_CONN_SUM,
                        T1.EPS_ECM_CONN_DENOM,
                        T1.EPS_ECM_IDLE_SUM,
                        T1.EPS_ECM_IDLE_DENOM,
                        T1.PROVIDE_SUBS_LOCATION_SUCC,
                        T1.PROVIDE_SUBS_LOCATION_ABN_SUCC,
                        T1.PROVIDE_SUBS_LOCATION_ABN_FAIL,
                        T1.LOCATION_SERVICE_REQ_SUCC,
                        T1.LOCATION_SERVICE_REQ_FAIL,
                        T1.SRVCC_PS_AND_CS_HANDOVER_SUCC,
                        T1.SRVCC_CS_HANDOVER_SUCC,
                        T1.OVERLOAD_CONTROL_DROP_PROC,
                        T1.SRVCC_PS_AND_CS_HO_3G_FAIL,
                        T1.SRVCC_CS_HO_3G_FAIL,
                        T1.SRVCC_CS_HO_2G_SUCC,
                        T1.SRVCC_CS_HO_2G_FAIL,
                        T1.SRVCC_EME_HO_3G_SUCC,
                        T1.SRVCC_EME_HO_3G_FAIL,
                        T1.SRVCC_EME_HO_2G_SUCC,
                        T1.SRVCC_EME_HO_2G_FAIL,
                        T1.SRVCC_3G_CANCELLED,
                        T1.SRVCC_2G_CANCELLED,
                        T1.SRVCC_EME_3G_CANCELLED,
                        T1.SRVCC_EME_2G_CANCELLED,
                        T1.EPS_LICENSE_REG_FAIL,
                        T1.EPS_LICENSE_BEARER_FAIL,
                        T1.EPS_PURGE_HSS_OK_SUCC,
                        T1.EPS_PURGE_HSS_NOK_SUCC
                FROM ' || p_source || ' T1
                LEFT OUTER JOIN ' || p_target || ' T3
                    ON T1.FINS_ID = T3.FINS_ID
                    AND T1.PERIOD_START_TIME = T3.PERIOD_START_TIME
                WHERE T1.PERIOD_START_TIME BETWEEN (SYSDATE - 14) AND SYSDATE
                AND T3.FINS_ID IS NULL
                AND T3.PERIOD_START_TIME IS NULL' ; --
    OPEN cur FOR sql_stmt;
    LOOP
      FETCH cur BULK COLLECT INTO t_array_col LIMIT p_limit_in;
      BEGIN
        FORALL i IN 1 .. t_array_col.COUNT SAVE EXCEPTIONS
          INSERT INTO PCOFNS_PS_UMLM_FLEXINS_RAW VALUES t_array_col (i);
        EXCEPTION
          WHEN OTHERS THEN
            -- Capture exceptions to perform operations DML
            l_errors := sql%bulk_exceptions.count;
            for i in 1 .. l_errors
            loop
                l_errno := sql%bulk_exceptions(i).error_code;
                l_msg   := sqlerrm(-l_errno);
                L_IDX   := SQL%BULK_EXCEPTIONS(I).ERROR_INDEX;
                
                PKG_ERROR_LOG_NEW.P_LOG_ERROR('P_IMP_LTE_USER_MME_LEVEL',
                                              L_ERRNO,
                                              L_MSG,
                                              'FINS_ID'||'->'||TO_CHAR(T_ARRAY_COL(L_IDX).FINS_ID)||' '||
                                              'PERIOD_START_TIME'||'->'||T_ARRAY_COL(L_IDX).PERIOD_START_TIME);
            end loop;
        -- end --
      END;
      EXIT WHEN cur%NOTFOUND;
    END LOOP;
    COMMIT;
    CLOSE cur;
    EXCEPTION
      WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE(SQLERRM);
        PKG_ERROR_LOG_NEW.P_LOG_ERROR('P_IMP_LTE_USER_MME_LEVEL',SQLCODE,SQLERRM,'P_SOURCE -> '||P_SOURCE||' P_TARGET -> '||P_TARGET);
  END P_IMP_LTE_USER_MME_LEVEL;
  
  PROCEDURE P_IMP_LTE_SGSAP(P_SOURCE IN VARCHAR2,P_TARGET IN VARCHAR2) IS
    sql_stmt VARCHAR2(4000);
    TYPE cur_typ IS REF CURSOR;
    cur CUR_TYP;
    -- LOG --
    l_errors number;
    l_errno    number;
    l_msg    varchar2(4000);
    l_idx    number;
    -- END LOG --
  
    TYPE T_ARRAY_TAB IS TABLE OF PCOFNS_PS_SGSM_FLEXINS_RAW%ROWTYPE;
    t_array_col t_array_tab;
  BEGIN
    sql_stmt := 'SELECT T1.FINS_ID,
                        T1.PERIOD_START_TIME,
                        T1.PERIOD_DURATION,
                        T1.SGSAP_UPLINK_SUCC,
                        T1.SGSAP_UPLINK_FAIL,
                        T1.SGSAP_DOWNLINK_SUCC,
                        T1.SGSAP_DOWNLINK_FAIL,
                        T1.CSFB_PAGING_ATTEMPT,
                        T1.CSFB_PAGING_FAIL
                 FROM ' || p_source || ' T1
                 LEFT OUTER JOIN ' || p_target || ' T3
                   ON T1.FINS_ID = T3.FINS_ID
                   AND T1.PERIOD_START_TIME = T3.PERIOD_START_TIME
                 WHERE T1.PERIOD_START_TIME BETWEEN (SYSDATE - 14) AND SYSDATE
                 AND T3.FINS_ID IS NULL
                 AND T3.PERIOD_START_TIME IS NULL' ;
    OPEN cur FOR sql_stmt;
  LOOP
    FETCH cur BULK COLLECT INTO t_array_col LIMIT p_limit_in;
    BEGIN
      FORALL i IN 1 .. t_array_col.COUNT SAVE EXCEPTIONS
        INSERT INTO PCOFNS_PS_SGSM_FLEXINS_RAW VALUES t_array_col (i);
      EXCEPTION
        WHEN OTHERS THEN
          -- Capture exceptions to perform operations DML
          l_errors := sql%bulk_exceptions.count;
          for i in 1 .. l_errors
          loop
              l_errno := sql%bulk_exceptions(i).error_code;
              l_msg   := sqlerrm(-l_errno);
              L_IDX   := SQL%BULK_EXCEPTIONS(I).ERROR_INDEX;
              
              PKG_ERROR_LOG_NEW.P_LOG_ERROR('P_IMP_LTE_SGSAP',
                                            L_ERRNO,
                                            L_MSG,
                                            'FINS_ID'||'->'||TO_CHAR(T_ARRAY_COL(L_IDX).FINS_ID)||' '||
                                            'PERIOD_START_TIME'||'->'||T_ARRAY_COL(L_IDX).PERIOD_START_TIME||' '||
                                            'PERIOD_DURATION -> '||TO_CHAR(T_ARRAY_COL(L_IDX).PERIOD_DURATION)||' '||
                                            'SGSAP_UPLINK_SUCC -> '||TO_CHAR(T_ARRAY_COL(L_IDX).SGSAP_UPLINK_SUCC)||' '||
                                            'SGSAP_UPLINK_FAIL -> '||TO_CHAR(T_ARRAY_COL(L_IDX).SGSAP_UPLINK_FAIL)||' '||
                                            'SGSAP_DOWNLINK_SUCC -> '||TO_CHAR(T_ARRAY_COL(L_IDX).SGSAP_DOWNLINK_SUCC)||' '||
                                            'SGSAP_DOWNLINK_FAIL -> '||TO_CHAR(T_ARRAY_COL(L_IDX).SGSAP_DOWNLINK_FAIL)||' '||
                                            'CSFB_PAGING_ATTEMPT -> '||TO_CHAR(T_ARRAY_COL(L_IDX).CSFB_PAGING_ATTEMPT)||' '||
                                            'CSFB_PAGING_FAIL -> '||TO_CHAR(T_ARRAY_COL(L_IDX).CSFB_PAGING_FAIL));
          end loop;
      -- end --
    END;
    EXIT WHEN cur%NOTFOUND;
  END LOOP;
  COMMIT;
  CLOSE cur;
  EXCEPTION
    WHEN OTHERS THEN
      --DBMS_OUTPUT.PUT_LINE(SQLERRM);
      PKG_ERROR_LOG_NEW.P_LOG_ERROR('P_IMP_LTE_SGSAP',SQLCODE,SQLERRM,'P_SOURCE -> '||P_SOURCE||' P_TARGET -> '||P_TARGET);
  END P_IMP_LTE_SGSAP;
  
  PROCEDURE P_IMP_LTE_COMPUTER_UNIT_LOAD(P_SOURCE IN VARCHAR2,P_TARGET IN VARCHAR2) IS
    sql_stmt VARCHAR2(4000);
    TYPE cur_typ IS REF CURSOR;
    cur CUR_TYP;
    -- LOG --
    l_errors number;
    l_errno    number;
    l_msg    varchar2(4000);
    l_idx    number;
    -- END LOG --
  
    TYPE T_ARRAY_TAB IS TABLE OF PCOFNS_PS_ULOAD_UNIT1_RAW%ROWTYPE;
    t_array_col t_array_tab;
  BEGIN
    sql_stmt := 'SELECT T1.FINS_ID,
                        T1.UNIT_ID,
                        T1.PERIOD_START_TIME,
                        T1.PERIOD_DURATION,
                        T1.COMP_AVERAGE_LOAD,
                        T1.COMP_PEAK_LOAD_PERCENT,
                        T1.COMP_PEAK_LOAD_DATE,
                        T1.COMP_PEAK_LOAD_TIME,
                        T1.COMP_INVALID_RECORD
                FROM ' || p_source || ' T1
                LEFT OUTER JOIN ' || p_target || ' T3
                   ON T1.FINS_ID = T3.FINS_ID
                   AND T1.PERIOD_START_TIME = T3.PERIOD_START_TIME
                WHERE T1.PERIOD_START_TIME BETWEEN (SYSDATE - 14) AND SYSDATE
                AND T3.FINS_ID IS NULL
                AND T3.PERIOD_START_TIME IS NULL' ;
    OPEN cur FOR sql_stmt;
  LOOP
    FETCH cur BULK COLLECT INTO t_array_col LIMIT p_limit_in;
    BEGIN
      FORALL i IN 1 .. t_array_col.COUNT SAVE EXCEPTIONS
        INSERT INTO PCOFNS_PS_ULOAD_UNIT1_RAW VALUES t_array_col (i);
    EXCEPTION
      WHEN OTHERS THEN
        -- Capture exceptions to perform operations DML
        l_errors := sql%bulk_exceptions.count;
        for i in 1 .. l_errors
        loop
            l_errno := sql%bulk_exceptions(i).error_code;
            l_msg   := sqlerrm(-l_errno);
            L_IDX   := SQL%BULK_EXCEPTIONS(I).ERROR_INDEX;
            
            PKG_ERROR_LOG_NEW.P_LOG_ERROR('P_IMP_LTE_SGSAP',
                                            L_ERRNO,
                                            L_MSG,
                                            'FINS_ID -> '||TO_CHAR(T_ARRAY_COL(L_IDX).FINS_ID)||' '||
                                            'PERIOD_START_TIME -> '||T_ARRAY_COL(L_IDX).PERIOD_START_TIME||' '||
                                            'UNIT_ID -> '||t_array_col(l_idx).UNIT_ID||' '||
                                            'PERIOD_DURATION -> '||TO_CHAR(T_ARRAY_COL(L_IDX).PERIOD_DURATION)||' '||
                                            'COMP_AVERAGE_LOAD -> '||TO_CHAR(T_ARRAY_COL(L_IDX).COMP_AVERAGE_LOAD)||' '||
                                            'COMP_PEAK_LOAD_PERCENT -> '||TO_CHAR(T_ARRAY_COL(L_IDX).COMP_PEAK_LOAD_PERCENT)||' '||
                                            'COMP_PEAK_LOAD_DATE -> '||TO_CHAR(T_ARRAY_COL(L_IDX).COMP_PEAK_LOAD_DATE)||' '||
                                            'COMP_PEAK_LOAD_TIME -> '||TO_CHAR(T_ARRAY_COL(L_IDX).COMP_PEAK_LOAD_TIME)||' '||
                                            'COMP_INVALID_RECORD -> '||TO_CHAR(T_ARRAY_COL(L_IDX).COMP_INVALID_RECORD));
        end loop;
    -- end --
    END;
    EXIT WHEN cur%NOTFOUND;
  END LOOP;
  COMMIT;
  CLOSE cur;
  EXCEPTION
    WHEN OTHERS THEN
      --DBMS_OUTPUT.PUT_LINE(SQLERRM);
      PKG_ERROR_LOG_NEW.P_LOG_ERROR('P_IMP_LTE_COMPUTER_UNIT_LOAD',SQLCODE,SQLERRM,'P_SOURCE -> '||P_SOURCE||' P_TARGET -> '||P_TARGET);
  END P_IMP_LTE_COMPUTER_UNIT_LOAD;
  
  PROCEDURE P_IMP_LTE_NETWORK_ELEMENTUSER(P_SOURCE IN VARCHAR2,P_TARGET IN VARCHAR2) IS
    sql_stmt VARCHAR2(4000);
    TYPE cur_typ IS REF CURSOR;
    cur CUR_TYP;
    -- LOG --
    l_errors number;
    l_errno    number;
    l_msg    varchar2(4000);
    l_idx    number;
    -- END LOG --
  
    TYPE T_ARRAY_TAB IS TABLE OF PCOFNS_PS_NEUM_FLEXINS_RAW%ROWTYPE;
    t_array_col t_array_tab;
  BEGIN
    sql_stmt := 'SELECT T1.FINS_ID,
                        T1.PERIOD_START_TIME,
                        T1.PERIOD_DURATION,
                        T1.NS_ATTACHED_USERS_MAX,
                        T1.NS_ATTACHED_USERS_SUM,
                        T1.NS_ATTACHED_USERS_DEN,
                        T1.NS_ROAMING_USERS_MAX,
                        T1.NS_ROAMING_USERS_SUM,
                        T1.NS_ROAMING_USERS_DEN,
                        T1.NS_DETACHED_USERS_MAX,
                        T1.NS_DETACHED_USERS_SUM,
                        T1.NS_DETACHED_USERS_DEN,
                        T1.NS_PURGE_ATTEMPT
                 FROM ' || p_source|| ' T1
                 LEFT OUTER JOIN ' || p_target || ' T3
                   ON T1.FINS_ID = T3.FINS_ID
                   AND T1.PERIOD_START_TIME = T3.PERIOD_START_TIME
                 WHERE T1.PERIOD_START_TIME BETWEEN (SYSDATE - 14) AND SYSDATE
                 AND T3.FINS_ID IS NULL
                 AND T3.PERIOD_START_TIME IS NULL';
    OPEN cur FOR sql_stmt;
  LOOP
    FETCH cur BULK COLLECT INTO t_array_col LIMIT p_limit_in;
    BEGIN
      FORALL i IN 1 .. t_array_col.COUNT SAVE EXCEPTIONS
        INSERT INTO PCOFNS_PS_NEUM_FLEXINS_RAW VALUES t_array_col (i);
      EXCEPTION
        WHEN OTHERS THEN
          -- Capture exceptions to perform operations DML
          l_errors := sql%bulk_exceptions.count;
          for i in 1 .. l_errors
          loop
              l_errno := sql%bulk_exceptions(i).error_code;
              l_msg   := sqlerrm(-l_errno);
              L_IDX   := SQL%BULK_EXCEPTIONS(I).ERROR_INDEX;
              
              PKG_ERROR_LOG_NEW.P_LOG_ERROR('P_IMP_LTE_NETWORK_ELEMENTUSER',
                                            L_ERRNO,
                                            L_MSG,
                                            'FINS_ID -> '||TO_CHAR(T_ARRAY_COL(L_IDX).FINS_ID)||' '||
                                            'PERIOD_START_TIME -> '||T_ARRAY_COL(L_IDX).PERIOD_START_TIME||' '||
                                            'PERIOD_DURATION -> '||TO_CHAR(T_ARRAY_COL(L_IDX).PERIOD_DURATION)||' '||
                                            'NS_ATTACHED_USERS_MAX -> '||TO_CHAR(T_ARRAY_COL(L_IDX).NS_ATTACHED_USERS_MAX)||' '||
                                            'NS_ATTACHED_USERS_SUM -> '||TO_CHAR(T_ARRAY_COL(L_IDX).NS_ATTACHED_USERS_SUM)||' '||
                                            'NS_ATTACHED_USERS_DEN -> '||TO_CHAR(T_ARRAY_COL(L_IDX).NS_ATTACHED_USERS_DEN)||' '||
                                            'NS_ROAMING_USERS_MAX -> '||TO_CHAR(T_ARRAY_COL(L_IDX).NS_ROAMING_USERS_MAX)||' '||
                                            'NS_ROAMING_USERS_SUM -> '||TO_CHAR(T_ARRAY_COL(L_IDX).NS_ROAMING_USERS_SUM)||' '||
                                            'NS_ROAMING_USERS_DEN -> '||TO_CHAR(T_ARRAY_COL(L_IDX).NS_ROAMING_USERS_DEN)||' '||
                                            'NS_DETACHED_USERS_MAX -> '||TO_CHAR(T_ARRAY_COL(L_IDX).NS_DETACHED_USERS_MAX)||' '||
                                            'NS_DETACHED_USERS_SUM -> '||TO_CHAR(T_ARRAY_COL(L_IDX).NS_DETACHED_USERS_SUM)||' '||
                                            'NS_DETACHED_USERS_DEN -> '||TO_CHAR(T_ARRAY_COL(L_IDX).NS_DETACHED_USERS_DEN)||' '||
                                            'NS_PURGE_ATTEMPT -> '||TO_CHAR(T_ARRAY_COL(L_IDX).NS_PURGE_ATTEMPT));

          end loop;
          -- end --
    END;
    EXIT WHEN cur%NOTFOUND;
  END LOOP;
  COMMIT;
  CLOSE cur;
  EXCEPTION
    WHEN OTHERS THEN
      --DBMS_OUTPUT.PUT_LINE(SQLERRM);
      PKG_ERROR_LOG_NEW.P_LOG_ERROR('P_IMP_LTE_NETWORK_ELEMENTUSER',SQLCODE,SQLERRM,'P_SOURCE -> '||P_SOURCE||' P_TARGET -> '||P_TARGET);
  END P_IMP_LTE_NETWORK_ELEMENTUSER;
  
  PROCEDURE P_CALL_IMPORT_PROCEDURES IS
  BEGIN
    P_IMP_LTE_MOBILITY_MANAGEMENT(P_SOURCE1_MOBILITY_MANAGEMENT,P_TARGET_MOBILITY_MANAGEMENT);
    P_IMP_LTE_SESSION_MANAGEMENT(P_SOURCE1_SESSION_MANAGEMENT,P_TARGET_SESSION_MANAGEMENT);
    P_IMP_LTE_MMDU_USER_LEVEL(P_SOURCE1_MMDU_USER_LEVEL,P_TARGET_MMDU_USER_LEVEL);
    P_IMP_LTE_USER_MME_LEVEL(P_SOURCE1_USER_MME_LEVEL,P_TARGET_USER_MME_LEVEL);
    P_IMP_LTE_SGSAP(P_SOURCE1_SGSAP,P_TARGET_SGSAP);
    P_IMP_LTE_COMPUTER_UNIT_LOAD(P_SOURCE1_COMPUTER_UNIT_LOAD,P_TARGET_COMPUTER_UNIT_LOAD);
    P_IMP_LTE_NETWORK_ELEMENTUSER(P_SOURCE1_NETWORK_ELEMENT_USER,P_TARGET_NETWORK_ELEMENT_USER);
  END P_CALL_IMPORT_PROCEDURES;
  --
  -- Fin procedimientos de importacion
  --
  FUNCTION F_MMMT RETURN FLNS_MMMT_TAB PIPELINED AS
  BEGIN
    FOR i IN (SELECT  PERIOD_START_TIME FECHA,
                       FINS_ID,
                       ROUND(sum(INTRATAU_WO_SGW_CHANGE_SUCC +
                           INTRAMME_TAU_SGW_CHG_SUCC +
                           INTRATAU_WO_SGW_CHANGE_FAIL +
                           EPS_PERIODIC_TAU_SUCC +
                           EPS_PERIODIC_TAU_FAIL) / avg(PERIOD_DURATION * 60),2) flns_3102d,
                       ROUND(sum(EPS_ATTACH_SUCC +
                           EPS_ATTACH_FAIL) / avg(PERIOD_DURATION * 60),2) flns_3100a,
                       ROUND(sum(EPS_DETACH) / avg(PERIOD_DURATION * 60),2) flns_3101a,
                       ROUND(sum(INTERMME_TAU_WO_SGW_CHG_SUCC +
                           INTERMME_TAU_IN_FAIL +
                           INTERMME_TAU_OUT_SUCC +
                           INTERMME_TAU_OUT_FAIL) / avg(PERIOD_DURATION * 60),2) flns_3113a,
                       ROUND(sum(EPS_INTER_TAU_OG_SUCC +
                           EPS_INTER_TAU_OG_FAIL) / avg(PERIOD_DURATION * 60),2) flns_3103b,
                       ROUND(sum(EPS_PATH_SWITCH_X2_SUCC +
                           EPS_X2HO_SGW_CHG_SUCC +
                           EPS_PATH_SWITCH_X2_FAIL +
                           EPS_S1HO_SUCC +
                           INTRAMME_S1HO_SGW_CHG_SUCC +
                           EPS_S1HO_FAIL +
                           INTRAMME_S1HO_SGW_CHG_FAIL) / avg(PERIOD_DURATION * 60),2) flns_3104b,
                       ROUND(sum(INTERMME_S1HO_WO_SGW_CHG_SUCC +
                           INTERMME_S1HO_IN_FAIL +
                           INTERMME_S1HO_OUT_SUCC +
                           INTERMME_S1HO_OUT_FAIL) / avg(PERIOD_DURATION * 60),2) flns_3114a,
                       ROUND(sum(EPS_PAGING_SUCC +
                           EPS_PAGING_FAIL) / avg(PERIOD_DURATION * 60),2) flns_3106a,
                       ROUND(sum(EPS_SERVICE_REQUEST_SUCC +
                           EPS_SERVICE_REQUEST_FAIL -
                           EPS_PAGING_SUCC) / avg(PERIOD_DURATION * 60),2) flns_3218a,
                       ROUND(sum(EPS_SERVICE_REQUEST_SUCC) / avg(PERIOD_DURATION * 60),2) flns_3115a,
                       ROUND(sum(ESR_MO_ATTEMPTS +
                           ESR_MT_ATTEMPTS +
                           ESR_MO_EMERGENCY_ATTEMPTS) / avg(PERIOD_DURATION * 60),2) flns_3108b
                  FROM PCOFNS_PS_MMMT_TA_RAW
                 --WHERE PERIOD_START_TIME BETWEEN TO_DATE (P_FINI,'DD.MM.YYYY') 
                 --                            AND TO_DATE (P_FFIN,'DD.MM.YYYY') + 83999/84000
                GROUP BY PERIOD_START_TIME, FINS_ID
                ) loop
      pipe row (FLNS_MMMT_OBJ(i.FECHA,i.FINS_ID,i.flns_3102d,i.flns_3100a,i.flns_3101a,i.flns_3113a ,i.flns_3103b ,
                              i.flns_3104b ,i.flns_3114a ,i.flns_3106a ,i.flns_3218a ,i.flns_3115a ,i.flns_3108b));
    END loop;
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_MMMT',SQLCODE,SQLERRM,null);
  END F_MMMT;
  
  FUNCTION F_SMMT  RETURN FLNS_SMMT_TAB PIPELINED AS
  BEGIN
    FOR i IN (SELECT PERIOD_START_TIME FECHA,
                     FINS_ID,
                     ROUND(sum(PDN_CONNECTIVITY_FAILED_UE +
                         PDN_CONNECTIVITY_FAILED_ENB +
                         DDBEARER_UEINIT_ACT_SUCC +
                         DDBEARER_UEINIT_ACT_FAIL +
                         DDBEARER_PGWINIT_ACT_SUCC +
                         DDBEARER_PGWINIT_ACT_FAIL) / avg(period_duration * 60),2) flns_3111a,
                     ROUND(sum(PDN_CONNECT_FAILED_COLLISION +
                         PDN_CONNECT_FAILED_UNSPECIFIED +
                         PDN_CONNECTIVITY_FAILED_MME +
                         PDN_CONNECTIVITY_FAILED_SGW +
                         PDN_CONNECTIVITY_SUCCESSFUL +
                         PDN_CONNECTIVITY_FAILED +
                         DDBEARER_UEINIT_DEACT_SUCC +
                         DDBEARER_UEINIT_DEACT_FAIL +
                         DDBEARER_MMEINIT_DEACT_SUCC +
                         DDBEARER_MMEINIT_DEACT_ABNORM +
                         DDBEARER_PGWINIT_DEACT_SUCC) / avg(period_duration * 60),2) flns_3112a,
                     ROUND(sum(GW_INIT_BEARER_MOD_SUCCESS +
                         GW_INIT_BEARER_MOD_FAILURE +
                         HSS_INIT_BEARER_MOD_SUCCESS +
                         HSS_INIT_BEARER_MOD_FAILURE) / avg(PERIOD_DURATION * 60),2) flns_3172a
                FROM PCOFNS_PS_SMMT_TA_RAW
               --WHERE PERIOD_START_TIME BETWEEN TO_DATE (P_FINI,'DD.MM.YYYY') 
                --                          AND TO_DATE (P_FFIN,'DD.MM.YYYY') + 83999/84000
              GROUP BY PERIOD_START_TIME, FINS_ID
                ) loop
      pipe row (FLNS_SMMT_OBJ(i.FECHA,i.FINS_ID ,i.flns_3111a ,i.flns_3112a ,i.flns_3172a));
    END loop;
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_SMMT',SQLCODE,SQLERRM,null);
  END F_SMMT;
  
  FUNCTION F_UMLM_MMMT  RETURN FLNS_UMLM_MMMT_TAB PIPELINED AS
  BEGIN
    FOR i IN (SELECT X.PERIOD_START_TIME FECHA,
                     X.FINS_ID,
                     ROUND(sum(EPS_TO_3G_GN_ISHO_SUCC +
                         EPS_TO_3G_GN_ISHO_FAIL +
                         EPS_TO_3G_GN_ISHO_TRGT_REJE +
                         EPS_TO_3G_GN_ISHO_ENB_CNCL -
                         SRVCC_PS_AND_CS_HANDOVER_SUCC -
                         SRVCC_PS_AND_CS_HO_3G_FAIL) / avg(Y.PERIOD_DURATION * 60),2) flns_3105b,
                     ROUND(sum(SRVCC_CS_HO_2G_SUCC +
                         SRVCC_CS_HO_2G_FAIL +
                         SRVCC_CS_HANDOVER_SUCC +
                         SRVCC_CS_HO_3G_FAIL +
                         SRVCC_PS_AND_CS_HANDOVER_SUCC +
                         SRVCC_PS_AND_CS_HO_3G_FAIL +
                         SRVCC_EME_HO_2G_SUCC +
                         SRVCC_EME_HO_2G_FAIL +
                         SRVCC_EME_HO_3G_SUCC +
                         SRVCC_EME_HO_3G_FAIL) / avg(X.PERIOD_DURATION * 60),2) flns_3287a
                FROM  PCOFNS_PS_UMLM_FLEXINS_RAW X, 
                      PCOFNS_PS_MMMT_TA_RAW Y
                WHERE 
                --X.PERIOD_START_TIME BETWEEN TO_DATE (P_FINI,'DD.MM.YYYY') 
                --                              AND TO_DATE (P_FFIN,'DD.MM.YYYY') + 83999/84000
                X.PERIOD_START_TIME = Y.PERIOD_START_TIME
                AND X.FINS_ID = Y.FINS_ID
                GROUP BY X.PERIOD_START_TIME, X.FINS_ID
                ) loop
      pipe row (FLNS_UMLM_MMMT_OBJ(i.FECHA,i.FINS_ID ,i.flns_3105b ,i.flns_3287a));
    END loop;
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_UMLM_MMMT',SQLCODE,SQLERRM,null);
  END F_UMLM_MMMT;
  
  FUNCTION FLNS_SGSM RETURN FLNS_SGSM_TAB PIPELINED AS
  BEGIN
    FOR i IN (SELECT PERIOD_START_TIME FECHA,
                     FINS_ID,
                     ROUND(sum(SGSAP_UPLINK_SUCC +
                         SGSAP_UPLINK_FAIL +
                         SGSAP_DOWNLINK_SUCC +
                         SGSAP_DOWNLINK_FAIL) / avg(PERIOD_DURATION * 60 * 4),2) flns_3109b
              FROM PCOFNS_PS_SGSM_FLEXINS_RAW
              --WHERE PERIOD_START_TIME BETWEEN TO_DATE (P_FINI,'DD.MM.YYYY') 
              --                            AND TO_DATE (P_FFIN,'DD.MM.YYYY') + 83999/84000
              GROUP BY PERIOD_START_TIME, FINS_ID
                ) loop
      pipe row (FLNS_SGSM_OBJ(i.FECHA ,i.FINS_ID ,i.flns_3109b));
    END loop;
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('FLNS_SGSM',SQLCODE,SQLERRM,null);
  END FLNS_SGSM;
  --
  /**
  * Para contener los resultados
  */
  --(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL)
  FUNCTION F_TPS  RETURN FLNS_TAB PIPELINED as
    --v_ffin VARCHAR2(10 CHAR) := '';
  BEGIN
    
    for i in (SELECT  MMMT.FECHA FECHA,
                      MMMT.FINS_ID FINS_ID,
                      (flns_3105b + flns_3102d + flns_3287a + flns_3100a + flns_3101a + flns_3111a + flns_3112a + 
                      flns_3172a + flns_3113a +  flns_3103b + flns_3104b + flns_3114a + flns_3106a + flns_3218a + 
                      flns_3115a + flns_3108b + flns_3109b) TPS
              FROM 
                  (SELECT FECHA ,
                          FINS_ID ,
                          flns_3102d ,
                          flns_3100a ,
                          flns_3101a ,
                          flns_3113a ,
                          flns_3103b ,
                          flns_3104b ,
                          flns_3114a ,
                          flns_3106a ,
                          flns_3218a ,
                          flns_3115a ,
                          flns_3108b
                  FROM TABLE(F_MMMT)) MMMT,
                  (SELECT FECHA ,
                          FINS_ID ,
                          flns_3111a ,
                          flns_3112a ,
                          flns_3172a
                  FROM TABLE(F_SMMT)) SMMT,
                  (SELECT FECHA ,
                          FINS_ID ,
                          flns_3105b ,
                          flns_3287a
                  FROM TABLE(F_UMLM_MMMT)) UMLM_MMMT,
                  (SELECT FECHA ,
                          FINS_ID ,
                          flns_3109b
                  FROM TABLE(FLNS_SGSM)) SGSM
              WHERE MMMT.FECHA = SMMT.FECHA
              AND   SMMT.FECHA = UMLM_MMMT.FECHA
              AND   UMLM_MMMT.FECHA = SGSM.FECHA
              AND   MMMT.FINS_ID = SMMT.FINS_ID
              AND   SMMT.FINS_ID = UMLM_MMMT.FINS_ID
              AND   UMLM_MMMT.FINS_ID = SGSM.FINS_ID
              ) loop
      pipe row (FLNS_OBJ(i.FECHA,i.FINS_ID,i.TPS));
    END loop;
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_TPS',SQLCODE,SQLERRM,null);
  end F_TPS;
  
  PROCEDURE P_TPS (RESULT_SET OUT SYS_REFCURSOR) IS
  BEGIN
    OPEN RESULT_SET FOR
      SELECT  FECHA,FINS_ID ,flns_value as flns_3110l
              ,B.name mme_name
      FROM TABLE(F_TPS) aa,
            (SELECT INT_ID,
                    NAME
            FROM MULTIVENDOR_OBJECTS
            WHERE element_type ='FLEXINS') B
      WHERE aa.FINS_ID = B.INT_ID;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        OPEN RESULT_SET FOR SELECT NULL FROM DUAL;
        
  END P_TPS;
  --
  -- CON FECHA
  --
  FUNCTION F_MMMT2 (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_MMMT_TAB PIPELINED AS
  BEGIN
    FOR i IN (SELECT  PERIOD_START_TIME FECHA,
                       FINS_ID,
                       ROUND(sum(INTRATAU_WO_SGW_CHANGE_SUCC +
                           INTRAMME_TAU_SGW_CHG_SUCC +
                           INTRATAU_WO_SGW_CHANGE_FAIL +
                           EPS_PERIODIC_TAU_SUCC +
                           EPS_PERIODIC_TAU_FAIL) / avg(PERIOD_DURATION * 60),2) flns_3102d,
                       ROUND(sum(EPS_ATTACH_SUCC +
                           EPS_ATTACH_FAIL) / avg(PERIOD_DURATION * 60),2) flns_3100a,
                       ROUND(sum(EPS_DETACH) / avg(PERIOD_DURATION * 60),2) flns_3101a,
                       ROUND(sum(INTERMME_TAU_WO_SGW_CHG_SUCC +
                           INTERMME_TAU_IN_FAIL +
                           INTERMME_TAU_OUT_SUCC +
                           INTERMME_TAU_OUT_FAIL) / avg(PERIOD_DURATION * 60),2) flns_3113a,
                       ROUND(sum(EPS_INTER_TAU_OG_SUCC +
                           EPS_INTER_TAU_OG_FAIL) / avg(PERIOD_DURATION * 60),2) flns_3103b,
                       ROUND(sum(EPS_PATH_SWITCH_X2_SUCC +
                           EPS_X2HO_SGW_CHG_SUCC +
                           EPS_PATH_SWITCH_X2_FAIL +
                           EPS_S1HO_SUCC +
                           INTRAMME_S1HO_SGW_CHG_SUCC +
                           EPS_S1HO_FAIL +
                           INTRAMME_S1HO_SGW_CHG_FAIL) / avg(PERIOD_DURATION * 60),2) flns_3104b,
                       ROUND(sum(INTERMME_S1HO_WO_SGW_CHG_SUCC +
                           INTERMME_S1HO_IN_FAIL +
                           INTERMME_S1HO_OUT_SUCC +
                           INTERMME_S1HO_OUT_FAIL) / avg(PERIOD_DURATION * 60),2) flns_3114a,
                       ROUND(sum(EPS_PAGING_SUCC +
                           EPS_PAGING_FAIL) / avg(PERIOD_DURATION * 60),2) flns_3106a,
                       ROUND(sum(EPS_SERVICE_REQUEST_SUCC +
                           EPS_SERVICE_REQUEST_FAIL -
                           EPS_PAGING_SUCC) / avg(PERIOD_DURATION * 60),2) flns_3218a,
                       ROUND(sum(EPS_SERVICE_REQUEST_SUCC) / avg(PERIOD_DURATION * 60),2) flns_3115a,
                       ROUND(sum(ESR_MO_ATTEMPTS +
                           ESR_MT_ATTEMPTS +
                           ESR_MO_EMERGENCY_ATTEMPTS) / avg(PERIOD_DURATION * 60),2) flns_3108b
                  FROM PCOFNS_PS_MMMT_TA_RAW
                 WHERE PERIOD_START_TIME BETWEEN TO_DATE (P_FINI,'DD.MM.YYYY') 
                                             AND TO_DATE (P_FFIN,'DD.MM.YYYY') + 83999/84000
                GROUP BY PERIOD_START_TIME, FINS_ID
                ) loop
      pipe row (FLNS_MMMT_OBJ(i.FECHA,i.FINS_ID,i.flns_3102d,i.flns_3100a,i.flns_3101a,i.flns_3113a ,i.flns_3103b ,
                              i.flns_3104b ,i.flns_3114a ,i.flns_3106a ,i.flns_3218a ,i.flns_3115a ,i.flns_3108b));
    END loop;
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_MMMT',SQLCODE,SQLERRM,null);
  END F_MMMT2;
  
  FUNCTION F_SMMT2 (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_SMMT_TAB PIPELINED AS
  BEGIN
    FOR i IN (SELECT PERIOD_START_TIME FECHA,
                     FINS_ID,
                     ROUND(sum(PDN_CONNECTIVITY_FAILED_UE +
                         PDN_CONNECTIVITY_FAILED_ENB +
                         DDBEARER_UEINIT_ACT_SUCC +
                         DDBEARER_UEINIT_ACT_FAIL +
                         DDBEARER_PGWINIT_ACT_SUCC +
                         DDBEARER_PGWINIT_ACT_FAIL) / avg(period_duration * 60),2) flns_3111a,
                     ROUND(sum(PDN_CONNECT_FAILED_COLLISION +
                         PDN_CONNECT_FAILED_UNSPECIFIED +
                         PDN_CONNECTIVITY_FAILED_MME +
                         PDN_CONNECTIVITY_FAILED_SGW +
                         PDN_CONNECTIVITY_SUCCESSFUL +
                         PDN_CONNECTIVITY_FAILED +
                         DDBEARER_UEINIT_DEACT_SUCC +
                         DDBEARER_UEINIT_DEACT_FAIL +
                         DDBEARER_MMEINIT_DEACT_SUCC +
                         DDBEARER_MMEINIT_DEACT_ABNORM +
                         DDBEARER_PGWINIT_DEACT_SUCC) / avg(period_duration * 60),2) flns_3112a,
                     ROUND(sum(GW_INIT_BEARER_MOD_SUCCESS +
                         GW_INIT_BEARER_MOD_FAILURE +
                         HSS_INIT_BEARER_MOD_SUCCESS +
                         HSS_INIT_BEARER_MOD_FAILURE) / avg(PERIOD_DURATION * 60),2) flns_3172a
                FROM PCOFNS_PS_SMMT_TA_RAW
                WHERE PERIOD_START_TIME BETWEEN TO_DATE (P_FINI,'DD.MM.YYYY') 
                                          AND TO_DATE (P_FFIN,'DD.MM.YYYY') + 83999/84000
              GROUP BY PERIOD_START_TIME, FINS_ID
                ) loop
      pipe row (FLNS_SMMT_OBJ(i.FECHA,i.FINS_ID ,i.flns_3111a ,i.flns_3112a ,i.flns_3172a));
    END loop;
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_SMMT',SQLCODE,SQLERRM,null);
  END F_SMMT2;
  
  FUNCTION F_UMLM_MMMT2 (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_UMLM_MMMT_TAB PIPELINED AS
  BEGIN
    FOR i IN (SELECT X.PERIOD_START_TIME FECHA,
                     X.FINS_ID,
                     ROUND(sum(EPS_TO_3G_GN_ISHO_SUCC +
                         EPS_TO_3G_GN_ISHO_FAIL +
                         EPS_TO_3G_GN_ISHO_TRGT_REJE +
                         EPS_TO_3G_GN_ISHO_ENB_CNCL -
                         SRVCC_PS_AND_CS_HANDOVER_SUCC -
                         SRVCC_PS_AND_CS_HO_3G_FAIL) / avg(Y.PERIOD_DURATION * 60),2) flns_3105b,
                     ROUND(sum(SRVCC_CS_HO_2G_SUCC +
                         SRVCC_CS_HO_2G_FAIL +
                         SRVCC_CS_HANDOVER_SUCC +
                         SRVCC_CS_HO_3G_FAIL +
                         SRVCC_PS_AND_CS_HANDOVER_SUCC +
                         SRVCC_PS_AND_CS_HO_3G_FAIL +
                         SRVCC_EME_HO_2G_SUCC +
                         SRVCC_EME_HO_2G_FAIL +
                         SRVCC_EME_HO_3G_SUCC +
                         SRVCC_EME_HO_3G_FAIL) / avg(X.PERIOD_DURATION * 60),2) flns_3287a
                FROM  PCOFNS_PS_UMLM_FLEXINS_RAW X, 
                      PCOFNS_PS_MMMT_TA_RAW Y
                WHERE 
                X.PERIOD_START_TIME BETWEEN TO_DATE (P_FINI,'DD.MM.YYYY') 
                                              AND TO_DATE (P_FFIN,'DD.MM.YYYY') + 83999/84000
                AND X.PERIOD_START_TIME = Y.PERIOD_START_TIME
                AND X.FINS_ID = Y.FINS_ID
                GROUP BY X.PERIOD_START_TIME, X.FINS_ID
                ) loop
      pipe row (FLNS_UMLM_MMMT_OBJ(i.FECHA,i.FINS_ID ,i.flns_3105b ,i.flns_3287a));
    END loop;
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_UMLM_MMMT',SQLCODE,SQLERRM,null);
  END F_UMLM_MMMT2;
  
  FUNCTION FLNS_SGSM2 (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_SGSM_TAB PIPELINED AS
  BEGIN
    FOR i IN (SELECT PERIOD_START_TIME FECHA,
                     FINS_ID,
                     ROUND(sum(SGSAP_UPLINK_SUCC +
                         SGSAP_UPLINK_FAIL +
                         SGSAP_DOWNLINK_SUCC +
                         SGSAP_DOWNLINK_FAIL) / avg(PERIOD_DURATION * 60 * 4),2) flns_3109b
              FROM PCOFNS_PS_SGSM_FLEXINS_RAW
              WHERE PERIOD_START_TIME BETWEEN TO_DATE (P_FINI,'DD.MM.YYYY') 
                                          AND TO_DATE (P_FFIN,'DD.MM.YYYY') + 83999/84000
              GROUP BY PERIOD_START_TIME, FINS_ID
                ) loop
      pipe row (FLNS_SGSM_OBJ(i.FECHA ,i.FINS_ID ,i.flns_3109b));
    END loop;
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('FLNS_SGSM',SQLCODE,SQLERRM,null);
  END FLNS_SGSM2;
  --
  /**
  * Para contener los resultados
  */
  --
  FUNCTION F_LTE_CORE_TPS_HOUR (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL)  RETURN FLNS_TAB PIPELINED as
    v_ffin VARCHAR2(10 CHAR) := '';
  BEGIN
    IF P_FFIN IS NULL THEN
      V_FFIN := P_FINI;
    ELSE
      V_FFIN := P_FFIN;
    END IF;
    
    for i in (SELECT  MMMT.FECHA FECHA,
                      MMMT.FINS_ID FINS_ID,
                      (flns_3105b + flns_3102d + flns_3287a + flns_3100a + flns_3101a + flns_3111a + flns_3112a + 
                      flns_3172a + flns_3113a +  flns_3103b + flns_3104b + flns_3114a + flns_3106a + flns_3218a + 
                      flns_3115a + flns_3108b + flns_3109b) TPS
              FROM 
                  (SELECT FECHA ,
                          FINS_ID ,
                          flns_3102d ,
                          flns_3100a ,
                          flns_3101a ,
                          flns_3113a ,
                          flns_3103b ,
                          flns_3104b ,
                          flns_3114a ,
                          flns_3106a ,
                          flns_3218a ,
                          flns_3115a ,
                          flns_3108b
                  FROM TABLE(F_MMMT2(P_FINI,V_FFIN))) MMMT,
                  (SELECT FECHA ,
                          FINS_ID ,
                          flns_3111a ,
                          flns_3112a ,
                          flns_3172a
                  FROM TABLE(F_SMMT2(P_FINI,V_FFIN))) SMMT,
                  (SELECT FECHA ,
                          FINS_ID ,
                          flns_3105b ,
                          flns_3287a
                  FROM TABLE(F_UMLM_MMMT2(P_FINI,V_FFIN))) UMLM_MMMT,
                  (SELECT FECHA ,
                          FINS_ID ,
                          flns_3109b
                  FROM TABLE(FLNS_SGSM2(P_FINI,V_FFIN))) SGSM
              WHERE MMMT.FECHA = SMMT.FECHA
              AND   SMMT.FECHA = UMLM_MMMT.FECHA
              AND   UMLM_MMMT.FECHA = SGSM.FECHA
              AND   MMMT.FINS_ID = SMMT.FINS_ID
              AND   SMMT.FINS_ID = UMLM_MMMT.FINS_ID
              AND   UMLM_MMMT.FINS_ID = SGSM.FINS_ID
              ) loop
      pipe row (FLNS_OBJ(i.FECHA,i.FINS_ID,i.TPS));
    END loop;
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_LTE_CORE_TPS_HOUR',SQLCODE,SQLERRM,null);
  end F_LTE_CORE_TPS_HOUR;
  --
  FUNCTION F_LTE_CORE_TPS_DAY (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED AS
  BEGIN
    FOR i IN (select  to_char(fecha,'dd.mm.yyyy') fecha,
                      FINS_ID,
                      SUM(flns_value) FLNS_DAY
              from table(F_LTE_CORE_TPS_HOUR(P_FINI,P_FFIN))
              GROUP BY to_char(fecha,'dd.mm.yyyy'),FINS_ID
              ) LOOP
      
      PIPE ROW (FLNS_OBJ(i.FECHA,i.FINS_ID,i.FLNS_DAY));
    END LOOP;
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_LTE_CORE_TPS_DAY',SQLCODE,SQLERRM,null);
  END F_LTE_CORE_TPS_DAY;
  
  
  PROCEDURE P_TPS2 (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL,RESULT_SET OUT SYS_REFCURSOR) IS
  BEGIN
    OPEN RESULT_SET FOR
      SELECT  FECHA,FINS_ID ,flns_value as flns_3110l
              ,B.name mme_name
      FROM TABLE(F_LTE_CORE_TPS_HOUR(P_FINI,P_FFIN)) aa,
            (SELECT INT_ID,
                    NAME
            FROM MULTIVENDOR_OBJECTS
            WHERE element_type ='FLEXINS') B
      WHERE aa.FINS_ID = B.INT_ID;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        OPEN RESULT_SET FOR SELECT NULL FROM DUAL;
        
  END P_TPS2;
  
END G_LTE_CORE;
/
