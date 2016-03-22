create or replace view FLNS_3110L_HOUR as
SELECT FECHA,FINS_ID ,flns_value as flns_3110l
,B.name mme_name
FROM TABLE(pkg_lte_core.F_TPS) aa,
(SELECT INT_ID,
               NAME
          FROM MULTIVENDOR_OBJECTS
         WHERE OBJECT_CLASS = 3766) B
WHERE aa.FINS_ID = B.INT_ID;
