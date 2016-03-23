
SELECT FECHA,FINS_ID ,flns_value as flns_3110l
,B.name mme_name
FROM TABLE(pkg_lte_core.F_TPS) aa,
(SELECT INT_ID,
               NAME
          FROM MULTIVENDOR_OBJECTS
         WHERE element_type ='FLEXINS') B
WHERE aa.FINS_ID = B.INT_ID
AND FECHA BETWEEN TO_DATE('01.03.2016','DD.MM.YYYY') AND TO_DATE('09.03.2016','DD.MM.YYYY');


SELECT /*CSV*/ FECHA,FINS_ID ,flns_value as flns_3110l
,B.name mme_name
FROM TABLE(pkg_lte_core.F_TPS2('09.03.2016','09.03.2016')) aa,
(SELECT INT_ID,
               NAME
          FROM MULTIVENDOR_OBJECTS
         WHERE element_type ='FLEXINS') B
WHERE aa.FINS_ID = B.INT_ID;


GRANT EXECUTE ON pkg_lte_core TO CTI8777;