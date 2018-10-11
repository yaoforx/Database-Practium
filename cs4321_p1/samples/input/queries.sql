--SELECT * FROM Sailors S, Reserves R WHERE S.A = R.G AND S.A < 3 AND R.H < 103;
--SELECT DISTINCT S.B, R.G FROM Sailors S, Reserves R WHERE S.A = R.G AND S.A < 3 ORDER BY S.B;
--SELECT S1.A, S2.A, S1.B FROM Sailors S1, Sailors S2 WHERE S1.A < S2.A AND S1.B < 200;
--SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G;
--SELECT * FROM Sailors, Reserves, Boats WHERE Sailors.A = Reserves.G AND Reserves.H = Boats.D;
SELECT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D ORDER BY S.C;

-------------
-- SELECT * FROM Sailors;
-- SELECT Sailors.A FROM Sailors;
-- SELECT S.A FROM Sailors S;
-- SELECT * FROM Sailors S WHERE S.A < 3;
-- SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G;
-- SELECT * FROM Sailors S1, Sailors S2 WHERE S1.A < S2.A;
-- SELECT DISTINCT R.G FROM Reserves R;
-- SELECT * FROM Sailors ORDER BY Sailors.B;