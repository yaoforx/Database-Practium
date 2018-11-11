-- SELECT B.F, B.D FROM Boats B ORDER BY B.D;
-- SELECT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D ORDER BY S.C;
-- SELECT DISTINCT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D ORDER BY S.C;
SELECT DISTINCT S.A, R.G
FROM Sailors S, Reserves R, Boats B
WHERE S.B = R.G AND S.A = B.D AND R.H <> B.D AND R.H < 100
ORDER BY S.A;