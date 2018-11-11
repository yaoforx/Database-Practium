-- SELECT DISTINCT S.A, R.G
-- FROM Sailors S, Reserves R, Boats B
-- WHERE S.B = R.G AND S.A = B.D AND R.H <> B.D AND R.H < 100
-- ORDER BY S.A;
SELECT S.A FROM Sailors S, Reserves R
WHERE S.B = R.G AND R.H < 100 AND S.A >= 100;