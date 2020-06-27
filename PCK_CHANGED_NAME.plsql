CREATE OR REPLACE PACKAGE PCK_CHANGED_NAME AS

PROCEDURE insert_redacted1_l_redacted1_node;
PROCEDURE insert_redacted1_o_redact7(pi_bulk_size IN NUMBER DEFAULT 50);
PROCEDURE insert_redacted1_l_redact8;
PROCEDURE insert_redacted1_l_pnr_redact4;
PROCEDURE insert_redacted1_l_pnr_redact4stack;
PROCEDURE clean_redact4s; 

END PCK_changed_name;
/
CREATE OR REPLACE PACKAGE BODY PCK_changed_name AS

v_tag		trace.tag%TYPE := 'PCK_changed_name';
v_lvl		trace.lvl%TYPE := 5;

PROCEDURE insert_redacted1_l_redacted1_node IS

BEGIN
	
	toolbox.trace('insert_redacted1_l_redacted1_node:START', v_tag, v_lvl);
	
	--TODO add trace for each procedure
	DELETE FROM mig_redacted1_l_redacted1_node;
	
	INSERT INTO mig_redacted1_l_redacted1_node(a_baseline, b_node, b_nodetype, prefix, b_node_description, b_emref)
	SELECT 
		T04.cencs, 
		CASE 
			WHEN cecrf = 'ME' THEN
				trim('DME' || RRF)
			WHEN cecrf = 'EQ' THEN
				trim(regexp_replace(RRF,'EQ',''))
			ELSE
				trim(rrf)
		END as rrf,      
		cecrf,
		T04.rel,
		nodpa || cpaei, 
		rencs
	FROM MIG_T3D104 T04, MIG_T3D114 T14
	WHERE T04.CENCS = T14.CENCS AND T04.REL = T14.REL
		AND upper(T04.cencs) like 'F%';
		
	commit;
	
	toolbox.trace('insert_redacted1_l_redacted1_node:END', v_tag, v_lvl);

END insert_redacted1_l_redacted1_node;

PROCEDURE insert_redacted1_o_redact7(pi_bulk_size IN NUMBER DEFAULT 50) IS

	CURSOR CUR IS
		SELECT DISTINCT cencs
		FROM mig_redacted2;

	TYPE type_c IS TABLE OF mig_redacted2.cencs%TYPE;
	tab_c type_c;

BEGIN

  toolbox.trace('insert_redacted1_o_redact7:START', v_tag, v_lvl);

  -- purge effectivities table
  DELETE FROM mig_redacted1_o_redact7;
  
  OPEN CUR;
  LOOP
	FETCH CUR BULK COLLECT INTO tab_c LIMIT pi_bulk_size;
    IF tab_c.COUNT > 0 THEN
     
		FORALL i IN tab_c.FIRST..tab_c.LAST
		INSERT INTO tmp_eff_old (ci, rangefrom, rangeto)
		SELECT
		  cencs || '~csxde~' || csxde || '~csxca~' || csxca || '~rel~' || rel,
		  nsrawx,
		  nsrawx
		FROM mig_redacted2
		WHERE cencs = tab_c(i);

		--needs be declared
		MERGE_SET_OF_RANGE;

		INSERT INTO mig_redacted1_o_redact7 (cacode, rangefrom, rangeto, linked_pnr)
		SELECT DISTINCT regexp_substr(ci,'^([A-Z]|[0-9])*'), 
		rangefrom, 
		rangeto,
		CASE WHEN regexp_replace(regexp_substr(ci,'(~csxde~).*(~csxca~)'), '(~csxde~)|(~csxca~)', '') IS NULL
				OR regexp_replace(regexp_substr(ci,'(~csxde~).*(~csxca~)'), '(~csxde~)|(~csxca~)', '') = '' THEN
			--REL || CSXCA
			regexp_replace(regexp_substr(ci,'(~rel~).*$'), '(~csxca~)|(~rel~)', '') || regexp_replace(regexp_substr(ci,'(~csxca~).*(~rel~)'), '(~csxca~)|(~rel~)', '')
		ELSE
			--REL || CSXCD
			regexp_replace(regexp_substr(ci,'(~rel~).*$'), '(~csxca~)|(~rel~)', '') || regexp_replace(regexp_substr(ci,'(~csxde~).*(~csxca~)'), '(~csxde~)|(~csxca~)', '') 
		END
		FROM tmp_eff_new;
  

		COMMIT;
                
	END IF;
	EXIT WHEN CUR%NOTFOUND;
  END LOOP;
  CLOSE CUR;

  toolbox.trace('insert_redacted1_o_redact7:END', v_tag, v_lvl);
  
END insert_redacted1_o_redact7;

PROCEDURE insert_redacted1_l_redact8 IS

BEGIN

	toolbox.trace('insert_redacted1_l_redact8:START', v_tag, v_lvl);
	
	--perform reinsert
	DELETE FROM mig_redacted1_l_redact8;

	INSERT INTO mig_redacted1_l_redact8(cacode, a_pnrnumber, b_lonumber, b_lotype)
	SELECT cencs, rel || csxca, substr(xawst,1,1) || ' ' || trim(regexp_replace(xawst, 'D[A-Z]{2}','')), substr(xawst,2,2)
	FROM mig_redacted3
	WHERE SUBSTR(REL,1,1) IN ('F','G') and substr(xawst,2,2) = 'EQ';
	
    INSERT INTO mig_redacted1_l_redact8(cacode, a_pnrnumber, b_lonumber, b_lotype)
	SELECT cencs, rel || csxca, xawst, substr(xawst,2,2)
	FROM mig_redacted3
	WHERE SUBSTR(REL,1,1) IN ('F','G') and substr(xawst,2,2) <> 'EQ';

	commit;
	
	toolbox.trace('insert_redacted1_l_redact8:END', v_tag, v_lvl);

END insert_redacted1_l_redact8;

PROCEDURE insert_mig_redacted1_l_pnr_redact4 IS

CURSOR b_redact4_cur IS
	select CENCS, REL, RRF, CSXDE, CRF, NSOTQ
	from mig_redact6
	WHERE SUBSTR(REL,1,1) IN ('F','G')
		--AND cencs = 'FB275' AND REL = 'F76110095   000' AND CSXDE like 'AF' AND crf = 'EL' and rrf like '%F76110095000%'
	group by CENCS, REL, RRF, CSXDE, CRF, NSOTQ;
	
b_redact4_rec	b_redact4_cur%ROWTYPE;
TYPE type_redact5_tbl IS TABLE OF VARCHAR2(500);
redact5_tbl type_redact5_tbl;
clean_bredact4_larray DBMS_UTILITY.LNAME_ARRAY;
clean_bredact4_tbl_n BINARY_INTEGER;
redact5_tmp varchar2(5000) := '';
bnode_tmp varchar2(100);
pnr_tmp varchar2(100);
mod_temp varchar2(50);
mp_temp varchar2(1550);
eq_mod varchar2(50);

BEGIN
	toolbox.trace('insert_redacted1_l_pnr_redact4:START', v_tag, v_lvl);

	DELETE FROM mig_redacted1_l_pnr_redact4;
	
	clean_redact4s;

	OPEN b_redact4_cur;

	LOOP
	FETCH b_redact4_cur INTO b_redact4_rec;
	EXIT WHEN b_redact4_cur%NOTFOUND;

		SELECT redact5 BULK COLLECT INTO redact5_tbl
		FROM mig_redact6
		WHERE cencs = b_redact4_rec.cencs AND
			REL = b_redact4_rec.REL AND
			RRF = b_redact4_rec.RRF AND
			CSXDE = b_redact4_rec.CSXDE AND
			SUBSTR(REL,1,1) IN ('F','G')
		ORDER BY nlg3 ASC; 
		
		pnr_tmp := b_redact4_rec.REL || b_redact4_rec.CSXDE;
		
		IF b_redact4_rec.CRF = 'EQ' THEN
			bnode_tmp := 'D  ' || ltrim(b_redact4_rec.RRF) || '      ' || b_redact4_rec.nsotq;
		ELSIF b_redact4_rec.CRF = 'ME' THEN 
			bnode_tmp := 'DME' || b_redact4_rec.RRF || '      ' || b_redact4_rec.nsotq;
		ELSE
			bnode_tmp := b_redact4_rec.RRF;
		END IF;
		
		--DBMS_OUTPUT.PUT_LINE('BEFORE redact5_tbl.COUNT');
		--for a given CENCS, REL, RRF, CSXDE there exists more than 1 record
		IF redact5_tbl.COUNT > 1 THEN
			--each record has potential for multiple redact4s, loop through all recs
			--and expand out the redact4s to invididual records
			FOR i IN 1..redact5_tbl.COUNT LOOP
				--clean up the redact5 before processing
				redact5_tmp := trim(redact5_tbl(i));
				redact5_tmp := trim(regexp_replace(redact5_tmp, 'OU', ''));
				--DBMS_OUTPUT.PUT_LINE('1 redact5_tmp=' || redact5_tmp);
				
				IF b_redact4_rec.CRF <> 'EL' AND b_redact4_rec.CRF <> 'EQ' THEN
					--DBMS_OUTPUT.PUT_LINE('2 redact5_tmp=' || redact5_tmp);
					redact5_tmp := trim(regexp_replace(redact5_tmp, '( )+', '_'));
					
				ELSIF (b_redact4_rec.CRF = 'EQ' OR b_redact4_rec.CRF = 'ME') AND ( regexp_like(trim(redact5_tmp), '^(M|MPM|MP)[0-9]+$') 
					OR regexp_like(trim(redact5_tmp), '^(M|MPM|MP)[0-9]+( ){1,2}S[0-9]+$') ) THEN	
					--DBMS_OUTPUT.PUT_LINE('2.1 redact5_tmp=' || redact5_tmp);
					null; --no prep cleaning required
					
				ELSIF b_redact4_rec.CRF = 'EQ' THEN
					--DBMS_OUTPUT.PUT_LINE('3 redact5_tmp=' || redact5_tmp);
					eq_mod := regexp_substr(trim(redact5_tmp), 'M[0-9]+');
					eq_mod := trim(regexp_replace(eq_mod, '[A-Z]+', ''));
					redact5_tmp := trim(regexp_replace(redact5_tmp, '^M[0-9]+', ''));
					redact5_tmp := trim(regexp_replace(redact5_tmp, '( )+', ','));
					redact5_tmp := regexp_replace(redact5_tmp, '(\(|\))', '');
					redact5_tmp := trim(regexp_replace(redact5_tmp, ',$', ''));
					--DBMS_OUTPUT.PUT_LINE('3.1 redact5_tmp=' || redact5_tmp);
				ELSE
					--DBMS_OUTPUT.PUT_LINE('4.1 redact5_tmp=' || redact5_tmp);
					--EL recs are not CSV, but use double/triple spaces between MPs
					redact5_tmp := trim(regexp_replace(redact5_tmp, '( )+', ','));
				END IF;
				redact5_tmp := trim(regexp_replace(redact5_tmp, ',$', ''));
				--if multiple redact4s exist per record break into individuals redact4s
				IF regexp_like(redact5_tmp, ',+') THEN
					--DBMS_OUTPUT.PUT_LINE('4.11 redact5_tmp=' || redact5_tmp);
					DBMS_UTILITY.COMMA_TO_TABLE( 
					   list => redact5_tmp,
					   tablen => clean_bredact4_tbl_n,
					   tab => clean_bredact4_larray);
					--DBMS_OUTPUT.PUT_LINE('4.2 redact5_tmp=' || redact5_tmp);
					FOR k IN 1..clean_bredact4_larray.COUNT-1 LOOP
						redact5_tmp := trim(clean_bredact4_larray(k));
						--DBMS_OUTPUT.PUT_LINE('4.3 redact5_tmp=' || redact5_tmp);
						--ADD HERE
						IF b_redact4_rec.CRF <> 'EL' AND b_redact4_rec.CRF <> 'EQ' THEN
							redact5_tmp := trim(regexp_replace(redact5_tmp, '^[A-Z]*', ''));
							redact5_tmp := trim(regexp_replace(redact5_tmp, '(_)+', '/'));
							redact5_tmp := trim(regexp_replace(redact5_tmp, 'PM', ''));
							redact5_tmp := regexp_replace(redact5_tmp, '(\(|\))', '');
							--DBMS_OUTPUT.PUT_LINE('5 redact5_tmp=' || redact5_tmp);
							INSERT INTO mig_redacted1_l_pnr_redact4(redacted1, a_pnrnumber, a_lotype, b_node, b_redact4)
							VALUES (b_redact4_rec.cencs, pnr_tmp, b_redact4_rec.crf, bnode_tmp, redact5_tmp); 
						ELSIF b_redact4_rec.CRF = 'EQ' THEN
							--DBMS_OUTPUT.PUT_LINE('(5.1 redact5_tmp=' || redact5_tmp);
							redact5_tmp := trim(regexp_replace(redact5_tmp, 'PM', ''));							
							redact5_tmp := eq_mod || '/' || redact5_tmp;
							--DBMS_OUTPUT.PUT_LINE('6 redact5_tmp=' || redact5_tmp);
							INSERT INTO mig_redacted1_l_pnr_redact4(redacted1, a_pnrnumber, a_lotype, b_node, b_redact4)
							VALUES (b_redact4_rec.cencs, pnr_tmp, b_redact4_rec.crf, bnode_tmp, redact5_tmp); 
						ELSE 
							--DBMS_OUTPUT.PUT_LINE('7 redact5_tmp=' || redact5_tmp);
							IF redact5_tmp not like 'P0156' THEN
								BEGIN
									SELECT modnumber INTO mod_temp FROM mig_o_redact4 WHERE mpnumber like redact5_tmp AND source = 'TARGET';
									redact5_tmp := mod_temp || '/' || redact5_tmp;
								EXCEPTION
									WHEN NO_DATA_FOUND THEN
										--just print the MPNUMBER
										null;
								END;
							ELSE
								redact5_tmp := '20011/P0156';
							END IF;
							--DBMS_OUTPUT.PUT_LINE('7.1 redact5_tmp=' || redact5_tmp);
							INSERT INTO mig_redacted1_l_pnr_redact4(redacted1, a_pnrnumber, a_lotype, b_node, b_redact4)
							VALUES (b_redact4_rec.cencs, pnr_tmp, b_redact4_rec.crf, bnode_tmp, redact5_tmp); 
						END IF;
						
					END LOOP;
					
				--otherwise only handle the 1 redact4 for the given record. This is usually
				--the case for the NLG3 = 0001, but can sometimes have >1 redact4
				ELSE
					--DBMS_OUTPUT.PUT_LINE('8 redact5_tmp=' || redact5_tmp);
					IF b_redact4_rec.CRF <> 'EL' AND b_redact4_rec.CRF <> 'EQ' THEN
						--DBMS_OUTPUT.PUT_LINE('9 redact5_tmp=' || redact5_tmp);
						redact5_tmp := regexp_replace(trim(regexp_replace(redact5_tmp, '^[A-Z]*', '')),'(_)+', '/');
						redact5_tmp := trim(regexp_replace(redact5_tmp, '(\(|\))', ''));
						redact5_tmp := trim(regexp_replace(redact5_tmp, 'PM', ''));
						
					ELSIF b_redact4_rec.CRF = 'EQ' AND NOT regexp_like(trim(redact5_tmp), '^(M|MPM|MP)[0-9]+$') 
						AND NOT regexp_like(trim(redact5_tmp), '^(M|MPM|MP)[0-9]+( ){1,2}S[0-9]+$') THEN
						--DBMS_OUTPUT.PUT_LINE('10 redact5_tmp=' || redact5_tmp);
						redact5_tmp := trim(regexp_replace(redact5_tmp, '(PM)', ''));							
						redact5_tmp := eq_mod || '/' || redact5_tmp;
					
					ELSIF (b_redact4_rec.CRF = 'EQ' OR b_redact4_rec.CRF = 'ME') AND regexp_like(trim(redact5_tmp), '^(M|MPM|MP)[0-9]+( ){1,2}S[0-9]+$') THEN
						--DBMS_OUTPUT.PUT_LINE('10.05 redact5_tmp=' || redact5_tmp);
						redact5_tmp := regexp_replace(trim(regexp_replace(redact5_tmp, '^[A-Z]*', '')),'( )+', '/');
						redact5_tmp := trim(regexp_replace(redact5_tmp, 'PM', ''));
						
					ELSIF (b_redact4_rec.CRF = 'EQ' OR b_redact4_rec.CRF = 'ME') AND regexp_like(trim(redact5_tmp), '^(M|MPM|MP)[0-9]+$') THEN
						--DBMS_OUTPUT.PUT_LINE('10.1 redact5_tmp=' || redact5_tmp);
						redact5_tmp := trim(regexp_replace(redact5_tmp, '(M|MPM)', ''));
						BEGIN
							SELECT LISTAGG(redact4, ',') WITHIN GROUP (ORDER BY redact4) INTO mp_temp
							FROM mig_o_redact4
							WHERE source = 'TARGET' AND modnumber = redact5_tmp;
							
							redact5_tmp := mp_temp;
						EXCEPTION
							WHEN NO_DATA_FOUND THEN
								--just print the MPNUMBER
								null;
						END;
					
					ELSE
						--DBMS_OUTPUT.PUT_LINE('11 redact5_tmp=' || redact5_tmp);
						IF redact5_tmp not like 'P0156' THEN
							BEGIN
								SELECT modnumber INTO mod_temp FROM mig_o_redact4 WHERE mpnumber like redact5_tmp AND source = 'TARGET';
								redact5_tmp := mod_temp || '/' || redact5_tmp;
							EXCEPTION
								WHEN NO_DATA_FOUND THEN
									--just print the MPNUMBER
									null;
							END;
						ELSE
							redact5_tmp := '20011/P0156';
						END IF;
					END IF;
					
					INSERT INTO mig_redacted1_l_pnr_redact4(redacted1, a_pnrnumber, a_lotype, b_node, b_redact4)
					VALUES (b_redact4_rec.cencs, pnr_tmp, b_redact4_rec.crf, bnode_tmp, redact5_tmp); 
					
				END IF;
			
			END LOOP;
			
			redact5_tmp := '';
		--for a given CENCS, REL, RRF, CSXDE there exists only 1 record
		ELSE
			redact5_tmp := trim(redact5_tbl(1));
			--DBMS_OUTPUT.PUT_LINE('12 redact5_tmp=' || redact5_tmp);
			IF regexp_like(redact5_tmp, ',+') THEN
				redact5_tmp := trim(regexp_replace(redact5_tmp, '( )+', '_'));
				redact5_tmp := trim(regexp_replace(redact5_tmp, ',$', ''));
				--DBMS_OUTPUT.PUT_LINE('13 redact5_tmp= ' || redact5_tmp);
				DBMS_UTILITY.COMMA_TO_TABLE( 
					   list => redact5_tmp,
					   tablen => clean_bredact4_tbl_n,
					   tab => clean_bredact4_larray);
					   
				FOR k IN 1..clean_bredact4_larray.COUNT-1 LOOP

					redact5_tmp := trim(clean_bredact4_larray(k));
					
					IF b_redact4_rec.CRF <> 'EL' THEN
						--DBMS_OUTPUT.PUT_LINE('14 redact5_tmp=' || redact5_tmp);
						redact5_tmp := trim(regexp_replace(redact5_tmp, '^[A-Z]*', ''));
						redact5_tmp := trim(regexp_replace(redact5_tmp, '(_)+', '/'));
						redact5_tmp := trim(regexp_replace(redact5_tmp, 'PM', ''));
						redact5_tmp := regexp_replace(redact5_tmp, '(\(|\))', '');
						
					ELSIF b_redact4_rec.CRF = 'EL' AND regexp_like(trim(redact5_tmp), '^(M|MPM|MP)[0-9]+_{1,2}S[0-9]+$') THEN
						--DBMS_OUTPUT.PUT_LINE('14.1 redact5_tmp=' || redact5_tmp);
						redact5_tmp := trim(regexp_replace(redact5_tmp, '^[A-Z]*', ''));
						redact5_tmp := trim(regexp_replace(redact5_tmp, '(_)+', '/'));
						redact5_tmp := trim(regexp_replace(redact5_tmp, 'PM', ''));
						redact5_tmp := regexp_replace(redact5_tmp, '(\(|\))', '');
					
					ELSE
						IF redact5_tmp not like 'P0156' THEN
							BEGIN
								SELECT modnumber INTO mod_temp FROM mig_o_redact4 WHERE mpnumber like redact5_tmp AND source = 'TARGET';
								redact5_tmp := mod_temp || '/' || redact5_tmp;
							EXCEPTION
								WHEN NO_DATA_FOUND THEN
									--just print the MPNUMBER
									null;
							END;
						ELSE
							redact5_tmp := '20011/P0156';
						END IF;
					END IF;
					--DBMS_OUTPUT.PUT_LINE('15 redact5_tmp=' || redact5_tmp);
					INSERT INTO mig_redacted1_l_pnr_redact4(redacted1, a_pnrnumber, a_lotype, b_node, b_redact4)
					VALUES (b_redact4_rec.cencs, pnr_tmp, b_redact4_rec.crf, bnode_tmp, redact5_tmp); 
				END LOOP;
			
			--1 rec with only MPs that are seperated by spaces or just one MP
			ELSE
				--DBMS_OUTPUT.PUT_LINE('16 redact5_tmp=' || redact5_tmp);
				IF b_redact4_rec.CRF <> 'EQ' AND NOT regexp_like(redact5_tmp, '(\(|\))', '') 
					AND NOT regexp_like(trim(redact5_tmp), '(M|MPM)[0-9]+( )+[A-Z]+[0-9]+') THEN
					
					redact5_tmp := trim(regexp_replace(redact5_tmp, '( )+', ',')); --multiple MPs are seperated by spaces
					
				ELSIF regexp_like(trim(redact5_tmp), '(M|MPM)[0-9]+( )+[A-Z]+[0-9]+') THEN
				
					redact5_tmp := trim(regexp_replace(redact5_tmp, '^[A-Z]*', ''));
					
				--for LR only	
				ELSIF regexp_like(trim(redact5_tmp), '(M|MPM)[0-9]+( )+\([A-Z]+[0-9]+\)') 
					AND (b_redact4_rec.CRF <> 'EQ' OR b_redact4_rec.CRF <> 'ME') THEN
					
					redact5_tmp := trim(regexp_replace(redact5_tmp, '(MPM|M)', ''));
					redact5_tmp := trim(regexp_replace(redact5_tmp, '(P|PM)', ''));
					redact5_tmp := trim(regexp_replace(redact5_tmp, '( )+', '/'));
				ELSE
					--DBMS_OUTPUT.PUT_LINE('16.1 redact5_tmp=' || redact5_tmp);
					eq_mod := regexp_substr(trim(redact5_tmp), 'M[0-9]+');
					eq_mod := trim(regexp_replace(eq_mod, '[A-Z]+', ''));
					redact5_tmp := trim(regexp_replace(redact5_tmp, '^M[0-9]+', ''));
					redact5_tmp := trim(regexp_replace(redact5_tmp, '( )+', ','));
				END IF;
				redact5_tmp := trim(regexp_replace(redact5_tmp, 'OU', ''));
				redact5_tmp := trim(regexp_replace(redact5_tmp, ',$', ''));
				redact5_tmp := trim(regexp_replace(redact5_tmp, '(\(|\))', ''));
				redact5_tmp := trim(regexp_replace(redact5_tmp, ',$', '')); --some records have extra spaces
				redact5_tmp := trim(regexp_replace(redact5_tmp, '(,){2,10}', '')); --some records have extra spaces
				IF regexp_like(redact5_tmp, ',+') THEN
					DBMS_UTILITY.COMMA_TO_TABLE( 
						   list => redact5_tmp,
						   tablen => clean_bredact4_tbl_n,
						   tab => clean_bredact4_larray);
					--DBMS_OUTPUT.PUT_LINE('17 redact5_tmp=' || redact5_tmp);
					FOR k IN 1..clean_bredact4_larray.COUNT-1 LOOP
						redact5_tmp := trim(clean_bredact4_larray(k));
						IF b_redact4_rec.CRF <> 'EL' AND b_redact4_rec.CRF <> 'EQ' THEN
							redact5_tmp := trim(regexp_replace(regexp_replace(redact5_tmp, '^[A-Z]*', ''),'( )+', '/'));
							redact5_tmp := trim(regexp_replace(redact5_tmp, 'MPM', ''));
							redact5_tmp := trim(regexp_replace(redact5_tmp, 'PM', ''));
							INSERT INTO mig_redacted1_l_pnr_redact4(redacted1, a_pnrnumber, a_lotype, b_node, b_redact4)
							VALUES (b_redact4_rec.cencs, pnr_tmp, b_redact4_rec.crf, bnode_tmp, redact5_tmp);
						--specific case for ltex like M20014 (PMP0005 PMP0090 )
						ELSIF b_redact4_rec.CRF = 'EQ' THEN
							--DBMS_OUTPUT.PUT_LINE('18 redact5_tmp=' || redact5_tmp);
							redact5_tmp := trim(regexp_replace(redact5_tmp, 'PM', ''));							
							redact5_tmp := eq_mod || '/' || redact5_tmp;
							INSERT INTO mig_redacted1_l_pnr_redact4(redacted1, a_pnrnumber, a_lotype, b_node, b_redact4)
							VALUES (b_redact4_rec.cencs, pnr_tmp, b_redact4_rec.crf, bnode_tmp, redact5_tmp);
						ELSE
							--DBMS_OUTPUT.PUT_LINE('19 redact5_tmp=' || redact5_tmp);
							redact5_tmp := trim(regexp_replace(redact5_tmp,'( )+', '/'));
							IF redact5_tmp not like 'P0156' THEN
								BEGIN
									SELECT modnumber INTO mod_temp FROM mig_o_redact4 WHERE mpnumber like redact5_tmp AND source = 'TARGET';
									redact5_tmp := mod_temp || '/' || redact5_tmp;
								EXCEPTION
									WHEN NO_DATA_FOUND THEN
										--just print the MPNUMBER
										null;
								END;
							ELSE
								redact5_tmp := '20011/P0156';
							END IF;
							INSERT INTO mig_redacted1_l_pnr_redact4(redacted1, a_pnrnumber, a_lotype, b_node, b_redact4)
							VALUES (b_redact4_rec.cencs, pnr_tmp, b_redact4_rec.crf, bnode_tmp, redact5_tmp);
							
						END IF;
					END LOOP;
				--exactly 1 formatted MOD/MP
				ELSE
					--DBMS_OUTPUT.PUT_LINE('20 redact5_tmp=' || redact5_tmp);
					IF b_redact4_rec.CRF <> 'EL' AND b_redact4_rec.CRF <> 'EQ' THEN
						redact5_tmp := trim(regexp_replace(regexp_replace(redact5_tmp, '^[A-Z]*', ''),'( )+', '/'));
						redact5_tmp := trim(regexp_replace(redact5_tmp, 'MPM', ''));
						redact5_tmp := trim(regexp_replace(redact5_tmp, 'PM', ''));
					
						INSERT INTO mig_redacted1_l_pnr_redact4(redacted1, a_pnrnumber, a_lotype, b_node, b_redact4)
						VALUES (b_redact4_rec.cencs, pnr_tmp, b_redact4_rec.crf, bnode_tmp, redact5_tmp);
					--specific case for ltex like M20014 (PMP0005 PMP0090 )
					ELSIF b_redact4_rec.CRF = 'EQ' AND NOT regexp_like(redact5_tmp, '^[0-9]+( )+[A-Z]{1,3}[0-9]+$') THEN
						redact5_tmp := trim(regexp_replace(redact5_tmp, 'PM', ''));							
						redact5_tmp := eq_mod || '/' || redact5_tmp;
						--DBMS_OUTPUT.PUT_LINE('21 redact5_tmp=' || redact5_tmp);
						INSERT INTO mig_redacted1_l_pnr_redact4(redacted1, a_pnrnumber, a_lotype, b_node, b_redact4)
						VALUES (b_redact4_rec.cencs, pnr_tmp, b_redact4_rec.crf, bnode_tmp, redact5_tmp);
						
					ELSIF (b_redact4_rec.CRF = 'EQ' OR b_redact4_rec.CRF = 'ME' OR b_redact4_rec.CRF = 'EL') AND regexp_like(redact5_tmp, '^[0-9]+( )+[A-Z]{1,3}[0-9]+$') THEN
						--DBMS_OUTPUT.PUT_LINE('21.1 redact5_tmp=' || redact5_tmp);
						redact5_tmp := trim(redact5_tmp);
						redact5_tmp := trim(regexp_replace(regexp_replace(redact5_tmp, '^[A-Z]*', ''),'( )+', '/'));
						redact5_tmp := trim(regexp_replace(redact5_tmp, 'MPM', ''));
						redact5_tmp := trim(regexp_replace(redact5_tmp, 'PM', ''));
						INSERT INTO mig_redacted1_l_pnr_redact4(redacted1, a_pnrnumber, a_lotype, b_node, b_redact4)
						VALUES (b_redact4_rec.cencs, pnr_tmp, b_redact4_rec.crf, bnode_tmp, redact5_tmp);
					ELSE
						redact5_tmp := trim(regexp_replace(redact5_tmp,'( )+', '/'));
						--DBMS_OUTPUT.PUT_LINE('22 redact5_tmp=' || redact5_tmp);
						IF redact5_tmp not like 'P0156' THEN
							BEGIN
								SELECT modnumber INTO mod_temp FROM mig_o_redact4 WHERE mpnumber like redact5_tmp AND source = 'TARGET';
								redact5_tmp := mod_temp || '/' || redact5_tmp;
							EXCEPTION
								WHEN NO_DATA_FOUND THEN
									--just print the MPNUMBER
									null;
							END;
						ELSE
							redact5_tmp := '20011/P0156';
						END IF;
						--DBMS_OUTPUT.PUT_LINE('23 redact5_tmp=' || redact5_tmp);
						INSERT INTO mig_redacted1_l_pnr_redact4(redacted1, a_pnrnumber, a_lotype, b_node, b_redact4)
						VALUES (b_redact4_rec.cencs, pnr_tmp, b_redact4_rec.crf, bnode_tmp, redact5_tmp);
						
					END IF;

				END IF;

			END IF;
		
		END IF;
		
	END LOOP;
	CLOSE b_redact4_cur;

	commit;
	toolbox.trace('insert_redacted1_l_pnr_redact4:END', v_tag, v_lvl);

END insert_redacted1_l_pnr_redact4;

PROCEDURE insert_redacted1_l_pnr_redact4stack IS

	CURSOR b_redact4stack_cur IS
		select CENCS, REL, RRF, CSXDE, CRF, NSOTQ
		from mig_redact6
		WHERE SUBSTR(REL,1,1) IN ('F','G')
			--AND cencs = 'FS001' AND REL = 'F54511055   000' AND CSXDE like 'AF' AND crf = 'ME' and rrf like '%54S400408%'
		group by CENCS, REL, RRF, CSXDE, CRF, NSOTQ;
		
	b_redact4stack_rec	b_redact4stack_cur%ROWTYPE;
	TYPE type_redact5_tbl IS TABLE OF VARCHAR2(500);
	redact5_tbl type_redact5_tbl;
	clean_bredact4stack_larray DBMS_UTILITY.LNAME_ARRAY;
	clean_bredact4stack_tbl_n BINARY_INTEGER;
	redact5_tmp varchar2(5000) := '';
	redact5_final varchar2(5000) := '';
	bnode_tmp varchar2(100);
	pnr_tmp varchar2(100);
	mod_temp varchar2(50);
	mod_list_tmp varchar2(4000);
	eq_mod varchar2(50);

BEGIN

	toolbox.trace('insert_redacted1_l_pnr_redact4stack:START', v_tag, v_lvl);

	DELETE FROM mig_redacted1_l_pnr_redact4stack;

	clean_redact4s;
	
	OPEN b_redact4stack_cur;

	LOOP
	FETCH b_redact4stack_cur INTO b_redact4stack_rec;
	EXIT WHEN b_redact4stack_cur%NOTFOUND;

		SELECT redact5 BULK COLLECT INTO redact5_tbl
		FROM mig_redact6
		WHERE cencs = b_redact4stack_rec.cencs AND
			REL = b_redact4stack_rec.REL AND
			RRF = b_redact4stack_rec.RRF AND
			CSXDE = b_redact4stack_rec.CSXDE AND
			SUBSTR(REL,1,1) IN ('F','G')
		ORDER BY nlg3 ASC; 
		
		pnr_tmp := b_redact4stack_rec.REL || b_redact4stack_rec.CSXDE;
		
		IF b_redact4stack_rec.CRF = 'EQ' THEN
			bnode_tmp := 'D  ' || ltrim(b_redact4stack_rec.RRF) || '      ' || b_redact4stack_rec.nsotq;
		ELSIF b_redact4stack_rec.CRF = 'ME' THEN 
			bnode_tmp := 'DME' || b_redact4stack_rec.RRF || '      ' || b_redact4stack_rec.nsotq;
		ELSE
			bnode_tmp := b_redact4stack_rec.RRF;
		END IF;
		--DBMS_OUTPUT.PUT_LINE('BEGIN redact5_tmp=' || redact5_tmp);
		--for a given CENCS, REL, RRF, CSXDE there exists more than 1 record
		IF redact5_tbl.COUNT > 1 THEN
			FOR i IN 1..redact5_tbl.COUNT LOOP
				redact5_tmp := redact5_tbl(i);
				IF regexp_like(redact5_tmp, 'OU') THEN				
					redact5_tmp := trim(regexp_replace(redact5_tmp, 'OU', ''));
					redact5_tmp := redact5_tmp || ',';
				END IF;
				--DBMS_OUTPUT.PUT_LINE('0 redact5_tmp=' || redact5_tmp);
 				IF b_redact4stack_rec.CRF <> 'EL' AND b_redact4stack_rec.CRF <> 'EQ' THEN
					redact5_tmp := trim(regexp_replace(redact5_tmp, '(\(|\))', '')); 
					redact5_tmp := trim(regexp_replace(redact5_tmp, '^[A-Z]*', ''));
					redact5_tmp := trim(regexp_replace(redact5_tmp, 'MPM', ''));
					redact5_tmp := trim(regexp_replace(redact5_tmp, '( )+', '/'));
					redact5_tmp := trim(regexp_replace(redact5_tmp, 'PM', ''));

					redact5_final := redact5_final || ',' || redact5_tmp;
					--DBMS_OUTPUT.PUT_LINE('0.1 redact5_final=' || redact5_final);
				ELSIF b_redact4stack_rec.CRF = 'EQ' AND regexp_like(redact5_tmp,'(M|MP)[0-9]+( )*\(( )*[A-Z]+[0-9]+( )*\)') THEN
					--eq_mod := regexp_substr(trim(redact5_tmp), 'M[0-9]+');
					--eq_mod := trim(regexp_replace(eq_mod, '[A-Z]+', ''));			
					--redact5_tmp := eq_mod || '/' || redact5_tmp;
					redact5_tmp := trim(regexp_replace(redact5_tmp, '^(M|MP)+', ''));
					redact5_tmp := trim(regexp_replace(redact5_tmp, 'PM', ' '));
					redact5_tmp := trim(regexp_replace(redact5_tmp, '(\(|\))', ''));
					redact5_tmp := trim(regexp_replace(redact5_tmp, '( )+', '/'));
					redact5_tmp := trim(regexp_replace(redact5_tmp, '/$', ''));
					redact5_tmp := trim(regexp_replace(redact5_tmp, '^/', ''));
					redact5_final := redact5_final || ',' || redact5_tmp;
					--DBMS_OUTPUT.PUT_LINE('0.2 redact5_final=' || redact5_final);
				-- Records with CRF='EL' need a MOD LOOKUP
				ELSIF b_redact4stack_rec.CRF = 'EQ' AND regexp_like(redact5_tmp,'(M|MP)+[0-9]+( )+\(([A-Z]+[0-9]+( )+)+\)') THEN
					--DBMS_OUTPUT.PUT_LINE('1 redact5_tmp=' || redact5_tmp);
					eq_mod := regexp_substr(trim(redact5_tmp), 'M[0-9]+');
					eq_mod := trim(regexp_replace(eq_mod, '[A-Z]+', ''));
					
					redact5_tmp := trim(regexp_replace(redact5_tmp, '^M[0-9]+', ''));
					redact5_tmp := trim(regexp_replace(redact5_tmp, '(\(|\))', ''));
					redact5_tmp := trim(regexp_replace(redact5_tmp, '( )+', ','));
					redact5_tmp := trim(regexp_replace(redact5_tmp, 'PM', eq_mod || '/'));
					
					redact5_final := redact5_final || ',' || redact5_tmp;
				ELSE
					redact5_tmp := trim(regexp_replace(redact5_tmp, '( )+', ','));
					--DBMS_OUTPUT.PUT_LINE('3 redact5_tmp=' || redact5_tmp);
					DBMS_UTILITY.COMMA_TO_TABLE( 
					   list => redact5_tmp,
					   tablen => clean_bredact4stack_tbl_n,
					   tab => clean_bredact4stack_larray);
					   
					FOR k IN 1..clean_bredact4stack_larray.COUNT-1 LOOP
						
						redact5_tmp := trim(clean_bredact4stack_larray(k));
						
						IF redact5_tmp not like 'P0156' THEN
							
							BEGIN
								
								SELECT modnumber INTO mod_temp FROM mig_o_redact4 WHERE mpnumber like redact5_tmp AND source = 'TARGET';
								IF k = 1 THEN 
									mod_list_tmp := mod_temp || '/' || redact5_tmp;
								ELSE
									mod_list_tmp := mod_list_tmp || ',' || mod_temp || '/' || redact5_tmp;
								END IF;
							
							EXCEPTION
								WHEN NO_DATA_FOUND THEN 
									mod_list_tmp := mod_list_tmp || ',' || redact5_tmp;
							END;
							
						ELSE
						
							redact5_tmp := '20011/P0156';
							IF k = 1 THEN 
								mod_list_tmp := redact5_tmp;
							ELSE
								mod_list_tmp := mod_list_tmp || ',' || redact5_tmp;
							END IF;
							
						END IF;

					END LOOP;
					--DBMS_OUTPUT.PUT_LINE('4 redact5_tmp=' || redact5_tmp);
					redact5_tmp := mod_list_tmp;
					redact5_final := redact5_final || ',' || redact5_tmp;
				END IF;
				
			END LOOP;
			
			redact5_final := trim(regexp_replace(redact5_final, '^(,)+', ''));
			redact5_final := trim(regexp_replace(redact5_final, '(,)+$', ''));
			INSERT INTO mig_redacted1_l_pnr_redact4stack(redacted1, a_pnrnumber, a_lotype, b_node, b_redact4stack)
			VALUES (b_redact4stack_rec.cencs, pnr_tmp, b_redact4stack_rec.crf, bnode_tmp, redact5_final); 
			
			redact5_final := '';
			redact5_tmp := '';
		
		--for a given CENCS, REL, RRF, CSXDE there exists only 1 record
		ELSE
			--DBMS_OUTPUT.PUT_LINE('5 redact5_tmp=' || redact5_tmp);
			redact5_tmp := trim(redact5_tbl(1));
			
			IF b_redact4stack_rec.CRF <> 'EL' THEN
			
				redact5_tmp := trim(regexp_replace(redact5_tmp, 'OU', '')); --remove OU text
				redact5_tmp := trim(regexp_replace(redact5_tmp, '(\(|\))', ''));
				redact5_tmp := trim(regexp_replace(regexp_replace(redact5_tmp, '^[A-Z]*', ''),'( )+', '/'));
				redact5_tmp := trim(regexp_replace(redact5_tmp, 'MPM', ''));
				redact5_tmp := trim(regexp_replace(redact5_tmp, 'PM', ''));
			
			-- Records with CRF='EL' need a MOD LOOKUP
			ELSE
				redact5_tmp := trim(regexp_replace(redact5_tmp, '( )+', ','));
				mod_list_tmp := '';
				mod_temp := '';
				

				DBMS_UTILITY.COMMA_TO_TABLE( 
				   list => redact5_tmp,
				   tablen => clean_bredact4stack_tbl_n,
				   tab => clean_bredact4stack_larray);
					   
				FOR k IN 1..clean_bredact4stack_larray.COUNT-1 LOOP

					redact5_tmp := trim(clean_bredact4stack_larray(k));
					redact5_tmp := trim(regexp_replace(redact5_tmp, '(\(|\))', ''));
					
					IF redact5_tmp not like 'P0156' THEN
						--DBMS_OUTPUT.PUT_LINE('6 redact5_tmp=' || redact5_tmp);
						BEGIN
						
							SELECT modnumber INTO mod_temp FROM mig_o_redact4 WHERE mpnumber like redact5_tmp AND source = 'TARGET';
							IF k = 1 THEN 
								mod_list_tmp := mod_temp || '/' || redact5_tmp;
							ELSE
								mod_list_tmp := mod_list_tmp || ',' || mod_temp || '/' || redact5_tmp;
							END IF;
						
						EXCEPTION
							WHEN NO_DATA_FOUND THEN 
								mod_list_tmp := mod_list_tmp || ',' || redact5_tmp;
						END;
						
					ELSE
					
						redact5_tmp := '20011/P0156';
						IF k = 1 THEN 
							mod_list_tmp := redact5_tmp;
						ELSE
							mod_list_tmp := mod_list_tmp || ',' || redact5_tmp;
						END IF;
						
					END IF;

				END LOOP;
				mod_list_tmp := trim(regexp_replace(mod_list_tmp, '( )+', ''));
				mod_list_tmp := trim(regexp_replace(mod_list_tmp, '^(,)+', ''));
				mod_list_tmp := trim(regexp_replace(mod_list_tmp, '(,)+$', ''));
				redact5_tmp := mod_list_tmp;
				--DBMS_OUTPUT.PUT_LINE('7 redact5_tmp=' || redact5_tmp);
				INSERT INTO mig_redacted1_l_pnr_redact4stack(redacted1, a_pnrnumber, a_lotype, b_node, b_redact4stack)
				VALUES (b_redact4stack_rec.cencs, pnr_tmp, b_redact4stack_rec.crf, bnode_tmp, redact5_tmp);
				
			END IF;

		END IF;
		
	END LOOP;

	CLOSE b_redact4stack_cur;
	
	--remove any double or triple commas
	UPDATE mig_redacted1_l_pnr_redact4stack
	SET b_redact4stack = regexp_replace(b_redact4stack, '(,){2,10}', ',')
	where regexp_like(b_redact4stack, '(,){2,10}');
	
	--update M40021 to 40021/S10000
	UPDATE mig_redacted1_l_pnr_redact4stack
	SET b_redact4stack = regexp_replace(b_redact4stack, 'M40021', '40021/S10000')
	where regexp_like(b_redact4stack, 'M40021');
	
	
	toolbox.trace('insert_redacted1_l_pnr_redact4stack:END', v_tag, v_lvl);

END insert_redacted1_l_pnr_redact4stack;


PROCEDURE clean_redact4s IS


BEGIN
	----------------------------
	--REDACTED
	----------------------------
	UPDATE mig_redact6
	SET redact5 = trim(regexp_replace(redact5, '(EQU)+', '')) 
	|| ' ' || (SELECT mig_o_redact4.MPNUMBER 
				FROM mig_o_redact4 
				WHERE source = 'TARGET' 
					AND trim(regexp_replace(redact5, '(M|EQU)+', '')) = modnumber)
	where SUBSTR(REL,1,1) IN ('F','G') AND
		redact5 = 'M40198 EQU';
		
	UPDATE mig_redact6
	SET redact5 = regexp_replace(trim(regexp_replace(redact5,'001$','')), 'MPI', 'MP')
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, 'MPI[0-9]+ S[0-9]{5}(001){1}');

	
	--business rule 40021 MOD should have S10000 MP
	UPDATE mig_redact6
	SET redact5 = 'M40021 S10000'
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, '(M|MPI|MDI|)40021');
		
	--business rule 40032 MOD should have S10021 MP
	UPDATE mig_redact6
	SET redact5 = 'M40032 S10021'
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, 'M40032 (PMS10021)') OR
		regexp_like(redact5, '^M40032$') OR
		regexp_like(redact5, 'M40032( )+[A-Z]{1,2}[0-9]+$');
	
	--business rule:
	--M40032	S10021
	--M40043	S10024
	UPDATE mig_redact6
	SET redact5 = 'M40032 S10021,M40043 S10024'
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, '^M40032,M40043$');
		
	--business rule:
	--M40032	S10021
	--M40042 	S10023
	UPDATE mig_redact6
	SET redact5 = 'M40032 S10021,M40042 S10023'
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, '^M40032,M40042 OU$');
		
	--business rule:
	--M40032	S10021
	--M40043	S10024
	UPDATE mig_redact6
	SET redact5 = 'M40032 S10021,M40043 S10024,MPM41207 S11242 OU'
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, '^M40032,M40043,MPM41207 S11242 OU$');
	
	
	--M40032	S10021
	--M40042 	S10023
	UPDATE mig_redact6
	SET redact5 = 'M40032 S10021,M40042 S10023,MPM41207 S11242 OU'
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, '^M40032,M40042,MPM41207 S11242 OU$');
	
	--M40032	S10021
	UPDATE mig_redact6
	SET redact5 = 'M40032 S10021,MPM41207 S11242 OU'
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, '^M40032,MPM41207 S11242 OU$');
	
	--M40032	S10021
	UPDATE mig_redact6
	SET redact5 = 'M40032 S10021,MPM41207 S11241,MPM42283 S11926 OU'
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, '^M40032,MPM41207 S11241,MPM42283 S11926 OU$');
	
	--M40032	S10021
	UPDATE mig_redact6
	SET redact5 = 'M40032 S10021,MPM42283 S11926'
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, '^M40032,MPM42283 S11926$');
	
	UPDATE mig_redact6
	SET redact5 = 'M40032 S10021' || substr(redact5,7)
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, '^M40032,(.)+$');
	
	--M40042	S10023
	UPDATE mig_redact6
	SET redact5 = regexp_replace(redact5, 'M40042', 'M40042 S10023')
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, '^M40032,(.)+$');
	
	--M40043	S10024
	UPDATE mig_redact6
	SET redact5 = regexp_replace(redact5, 'M40043', 'M40043 S10024')
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, '^M40032,(.)+$');
	
	--M40059	S10026
	UPDATE mig_redact6
	SET redact5 = regexp_replace(redact5, 'M40059', 'M40059 S10026')
	where SUBSTR(REL,1,1) IN ('F','G') AND
		(regexp_like(redact5, '^M40059,') OR regexp_like(redact5, ',M40059,') OR regexp_like(redact5, ',M40059$'));
		
	--M40061	S10028
	UPDATE mig_redact6
	SET redact5 = regexp_replace(redact5, 'M40061', 'M40061 S10028')
	where SUBSTR(REL,1,1) IN ('F','G') AND
		(regexp_like(redact5, '^M40061,') OR regexp_like(redact5, ',M40061,') OR regexp_like(redact5, ',M40061$')
			OR regexp_like(redact5, 'M40061 OU')
		);
		
	--M40062	S10029
	UPDATE mig_redact6
	SET redact5 = regexp_replace(redact5, 'M40062', 'M40062 S10029')
	where SUBSTR(REL,1,1) IN ('F','G') AND
		(regexp_like(redact5, '^M40062,') OR regexp_like(redact5, ',M40062,') OR regexp_like(redact5, ',M40062$')
			OR regexp_like(redact5, 'M40062 OU')
		);
	
	--M40198	S10045
	UPDATE mig_redact6
	SET redact5 = regexp_replace(redact5, 'M40198', 'M40198 S10045')
	where SUBSTR(REL,1,1) IN ('F','G') AND
		(regexp_like(redact5, '^M40198,') OR regexp_like(redact5, ',M40198,') OR regexp_like(redact5, ',M40198$')
			OR regexp_like(redact5, 'M40198 OU')
		);
		
	--M40306	S10047
	UPDATE mig_redact6
	SET redact5 = regexp_replace(redact5, 'M40306', 'M40306 S10047')
	where SUBSTR(REL,1,1) IN ('F','G') AND
		(regexp_like(redact5, '^M40306,') OR regexp_like(redact5, ',M40306,') OR regexp_like(redact5, ',M40306$')
			OR regexp_like(redact5, 'M40306 OU')
		);
		
	--M40308	S10048
	UPDATE mig_redact6
	SET redact5 = regexp_replace(redact5, 'M40308', 'M40308 S10048')
	where SUBSTR(REL,1,1) IN ('F','G') AND
		(regexp_like(redact5, '^M40308,') OR regexp_like(redact5, ',M40308,') OR regexp_like(redact5, ',M40308$')
			OR regexp_like(redact5, 'M40308 OU')
		);
		
	--M41057	S10632
	UPDATE mig_redact6
	SET redact5 = regexp_replace(redact5, 'M41057', 'M41057 S10632')
	where SUBSTR(REL,1,1) IN ('F','G') AND
		(regexp_like(redact5, '^M41057,') OR regexp_like(redact5, ',M41057,') OR regexp_like(redact5, ',M41057$')
			OR regexp_like(redact5, 'M41057 OU')
		);
		
	--M41334	S11250
	UPDATE mig_redact6
	SET redact5 = regexp_replace(redact5, 'M41334', 'M41334 S11250')
	where SUBSTR(REL,1,1) IN ('F','G') AND
		(regexp_like(redact5, '^M41334,') OR regexp_like(redact5, ',M41334,') OR regexp_like(redact5, ',M41334$')
			OR regexp_like(redact5, 'M41334 OU')
		);
		
	--S10000  S10037Z S12571  S15199
	UPDATE mig_redact6
	SET redact5 = 'M40021 S10000,M40056 S10037,M43449 S12571,M48657 S15199'
	where SUBSTR(REL,1,1) IN ('F','G') AND redact5 = 'S10000  S10037Z S12571  S15199';
		
	
	--M40021	S10000
	--M40056	S10037Z
	--M43449	S12571
	--M44260	S12892
	UPDATE mig_redact6
	SET redact5 = 'M40021 S10000,M40056 S10037,M43449 S12571'
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, '^S10000  S10037Z S12571$');
		
	UPDATE mig_redact6
	SET redact5 = 'M40021 S10000,M40056 S10037,M43449 S12571,M44260 S12892'
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, '^S10000  S10037Z S12571  S12892$'); 
	
	--M40306	S10047
	UPDATE mig_redact6
	SET redact5 = 'M40306 S10047'
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, '^M40306 EQU$');
	
	UPDATE mig_redact6
	SET redact5 = 'M40306 S10047,MPM43618 S12627 OU'
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, '^M40306,MPM43618 S12627 OU$');
	
	
	--M40308	S10048
	UPDATE mig_redact6
	SET redact5 = 'M40308 S10048'
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, '^M40308$');
	
	UPDATE mig_redact6
	SET redact5 = 'M40308 S10048,MPM43618 S12627'
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, '^M40308,MPM43618 S12627$');
		
	UPDATE mig_redact6
	SET redact5 = 'M40021 S10000,M40056 S10037,M43449 S12571,M48657 S15199'
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, '^S10000  S10037Z S12571  S15199$');
		
	UPDATE mig_redact6
	SET redact5 = 'M42283 S11715,M42283 S11926'
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, '^M42283 \(PMS11715 PMS11926 \)$');
		
	UPDATE mig_redact6
	SET redact5 = 'M40032 S10021,M40032 S11554'
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, '^M40032 \(PMS10021 PMS11554 \)$');
		
	UPDATE mig_redact6
	SET redact5 = 'M40032 S10021,M40032 S10395'
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, '^M40032 \(PMS10021 PMS10395 \)$');
		
	UPDATE mig_redact6
	SET redact5 = 'M45468 S13666,M45468 S13994'
	where SUBSTR(REL,1,1) IN ('F','G') AND
		regexp_like(redact5, '^M45468 \(PMS13666 PMS13994 \)$');

	
	commit;
	----------------------------
	--REDACTED
	----------------------------

END;


END PCK_changed_name;