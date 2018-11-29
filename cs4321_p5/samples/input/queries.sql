--SELECT DISTINCT S.A, R.G FROM Sailors S, Reserves R, Boats B WHERE S.B = R.G AND S.A = B.D AND R.H <> B.D AND R.H < 100 ORDER BY S.A;
SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND C.CV>S.SV AND D.DV>=C.CV AND S.SV=P.PV
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND S.SV>D.DV AND P.PV>S.SV AND P.PV!=C.CV
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND C.CV<P.PV AND C.CV=S.SV AND P.PV<=D.DV
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND C.CV!=P.PV AND C.CV<D.DV AND P.PV>=S.SV
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND P.PV<S.SV AND P.PV<C.CV AND S.SV>=D.DV
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND D.DV>C.CV AND P.PV=S.SV
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND S.SV<C.CV AND D.DV<=P.PV
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND S.SV!=C.CV AND C.CV<D.DV AND D.DV=P.PV
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND C.CV<=P.PV AND P.PV>=D.DV AND C.CV>S.SV
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND S.SV!=C.CV AND P.PV!=S.SV AND C.CV<=D.DV
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND P.PV>S.SV AND D.DV>=C.CV
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND C.CV>=D.DV AND D.DV=S.SV AND P.PV>=C.CV
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND C.CV<P.PV AND S.SV<=C.CV AND S.SV=D.DV
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND D.DV<=C.CV AND S.SV<=P.PV
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND P.PV!=S.SV AND P.PV=D.DV AND D.DV!=C.CV
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND C.CV>D.DV AND S.SV<=D.DV AND P.PV<S.SV
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND C.CV!=S.SV AND P.PV<D.DV
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND S.SV>P.PV AND S.SV<=D.DV AND D.DV!=C.CV
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND C.CV<D.DV AND S.SV<P.PV
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND C.CV<D.DV AND P.PV<D.DV AND P.PV>=S.SV
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND C.CV!=49 AND S.SV<=15 AND D.DV>27 AND P.PV<=3
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND P.PV>=20 AND S.SV<=46 AND C.CV>=14 AND D.DV!=40
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND D.DV!=34 AND C.CV!=1 AND P.PV=43 AND S.SV>0
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND S.SV!=2 AND D.DV>23 AND P.PV!=13 AND C.CV>11
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND C.CV<21 AND S.SV<35 AND D.DV=19 AND P.PV<38
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND C.CV>=11 AND S.SV=20 AND P.PV<=6 AND D.DV=12
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND D.DV>33 AND C.CV=7 AND S.SV!=28 AND P.PV<34
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND C.CV<39 AND S.SV<6 AND D.DV>=17 AND P.PV>=25
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND D.DV>=36 AND S.SV!=49 AND P.PV>22 AND C.CV>=40
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND C.CV>=36 AND D.DV<=35 AND P.PV=40 AND S.SV<=49
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND C.CV!=27 AND D.DV>28 AND P.PV<=44 AND S.SV<15
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND S.SV>=22 AND D.DV!=50 AND C.CV<=10 AND P.PV!=30
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND D.DV<=40 AND S.SV>=47 AND C.CV!=8 AND P.PV>6
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND C.CV!=22 AND P.PV<4 AND D.DV<29 AND S.SV!=46
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND P.PV<34 AND D.DV>=28 AND S.SV<=50 AND C.CV>18
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND P.PV=35 AND D.DV!=31 AND C.CV<=28 AND S.SV<13
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND S.SV<44 AND P.PV>=40 AND D.DV>25 AND C.CV<=35
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND D.DV<=19 AND P.PV<=1 AND S.SV<44 AND C.CV<23
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND D.DV!=19 AND S.SV<38 AND C.CV<=3 AND P.PV=19
-- SELECT * FROM LINEORDER L, CUSTOMER C, PART P, SUPPLIER S, DATE D WHERE L.CKEY=C.CKEY AND L.PKEY=P.PKEY AND L.SKEY=S.SKEY AND L.DKEY=D.DKEY AND D.DV!=31 AND P.PV!=12 AND C.CV<41 AND S.SV>=8