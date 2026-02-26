# Specifikacija projekta
## Analiza šahovskih partija – paketna i obrada podataka u realnom vremenu

---

## 1. Domen projekta

Domen projekta je **analiza šahovskih partija** korišćenjem velikih skupova podataka, sa fokusom na:
- istorijske podatke o završenim partijama (batch / paketna obrada),
- novopristigle partije koje se pojavljuju tokom vremena (simulirana ili stvarna obrada u realnom vremenu).

Podaci uključuju informacije o partijama kao što su:
- igrači,
- rejting,
- datum i tempo igre,
- potezi partije (PGN),
- ishod partije (pobeda, poraz, remi),
- dodatne metrike izvedene analizom poteza (npr. evaluacije poteza).

---

## 2. Motivacija

Šah generiše ogromnu količinu strukturisanih i vremenski obeleženih podataka, što ga čini idealnim domenom za:
- analizu trendova,
- poređenje istorijskih i aktuelnih obrazaca igre,
- primenu tehnika obrade tokova podataka.

Motivacija projekta je da se:
- ispita kako se stil i uspešnost igrača menjaju tokom vremena,
- kombinuju istorijski podaci sa novopristiglim partijama,
- demonstrira razlika i komplementarnost između paketne obrade i obrade u realnom vremenu.

---

## 3. Ciljevi projekta

Glavni ciljevi projekta su:
- analiza velikog istorijskog skupa šahovskih partija pomoću paketne obrade,
- primena obrade podataka u realnom vremenu nad novim partijama,
- otkrivanje statističkih obrazaca i trendova u igri igrača,
- poređenje aktuelnih performansi igrača sa njihovim istorijskim podacima,
- implementacija kompleksnih transformacija nad tokovima podataka (windowing, agregacije, spajanje tokova).

---

## 4. Skup podataka

### 4.1 Istorijski (paketni) podaci

# Opis skupa podataka

Ovaj projekat koristi **više komplementarnih šahovskih dataset-ova u PGN formatu**, koji zajedno omogućavaju analizu istorijskih trendova. Svi dataset-ovi sadrže partije igrača visokog rejtinga (≈2400+), ali se razlikuju po izvoru, tipu partija i dostupnosti dodatnih metapodataka.

Link do dataseta **`[https://www.kaggle.com/datasets/chessmontdb/chessmont-big-dataset]`**(https://www.kaggle.com/datasets/chessmontdb/chessmont-big-dataset)

---

## 1. Lichess dataset sa evaluacijama  
**`lichess-2400-eval.pgn.zst`** i **`lichess-2400.pgn.zst`**

- **`lichess-2400-eval.pgn.zst`** – partije koje **imaju engine evaluacije poteza (Stockfish)**; čine **oko 20% ukupnog skupa**.

- **`lichess-2400.pgn.zst`** – partije **bez engine evaluacija**; predstavlja **sve partije** do maja 2025.


### Ključne karakteristike
- Partije raznih tempa (bullet, blitz, rapid...)
- Učesnici su igrači sa rejtingom oko **2400 i više**
- Svaki potez u eval datasetu sadrži **engine evaluaciju**:
  - numeričke vrednosti (npr. `[%eval 0.84]`)
  - procene mata (`[%eval #3]`, `#5`, itd.)
  - Svaki potez sadrži informaciju o satu:
  - `[%clk 0:04:59.1]`
- Standardni PGN tagovi:
  - `Event` – naziv događaja ili tipa partije (turnir, online meč, rejting partija)
  - `Site` – mesto ili platforma na kojoj je partija odigrana (grad ili online sajt)
  - `UTCDate` – datum odigravanja partije u UTC formatu (YYYY.MM.DD)
  - `UTCTime` – vreme početka partije u UTC formatu (HH:MM:SS)
  - `WhiteElo` – ELO rejting igrača koji igra belim figurama
  - `BlackElo` – ELO rejting igrača koji igra crnim figurama
  - `ECO` – kod otvaranja prema ECO (Encyclopaedia of Chess Openings) klasifikaciji
  - `Opening` – puni naziv šahovskog otvaranja
  - `TimeControl` – kontrola vremena partije (npr. 60+0, 300+2)
  - `Termination` – način na koji je partija završena (šah-mat, predaja, istek vremena, itd.)

---

## 2. Chess.com dataset sa vremenima po potezu  

Ovaj dataset sadrži **online partije sa platforme Chess.com**, uključujući **tačne podatke o vremenu preostalom na satu za svaki potez**.

### Ključne karakteristike
- Online partije (blitz, rapid ...)
- Učesnici su visoko-rejtingovani igrači (često 2400+)
- Svaki potez sadrži informaciju o satu:
  - `[%clk 0:04:59.1]`
- Dodatni PGN tagovi:
  - **`StartTime` / `EndTime`** – vreme početka i završetka partije (UTC)
  - **`Termination`** – način završetka partije (npr. resignation, time forfeit, checkmate)
  - **`Link`** – URL ka partiji na platformi (Chess.com ili Lichess)
  - **`CurrentPosition`** – FEN notacija trenutne pozicije (koristi se za live partije ili delimične snimke)

### Ograničenja
- Ne sadrži engine evaluacije

---

## 3. TWIC (The Week In Chess) dataset  
**`twic.pgn.zst`**

TWIC dataset sadrži **klasične turnirske partije vrhunskih profesionalnih igrača**, prikupljene iz međunarodnih turnira.

### Ključne karakteristike
- Partije sa zvaničnih turnira (FIDE i slični)
- Učesnici su uglavnom **velemajstori (GM)** i super-velemajstori
- Fokus na **klasični tempo igre**
- Bogati metapodaci:
  - **`WhiteTitle` / `BlackTitle`** – titula igrača (GM, IM, FM, itd.)
  - **`WhiteFideId` / `BlackFideId`** – jedinstveni FIDE ID igrača
  - **`Event`** – naziv turnira
  - **`Round`** – broj runde unutar turnira
  - **`EventDate`** – datum održavanja partije
  - **`Opening`** – naziv otvaranja
  - **`Variation`** – specifična varijacija otvaranja
  - **`ECO`** – ECO šifra otvaranja (Encyclopaedia of Chess Openings)

### Ograničenja
- Ne sadrži engine evaluacije
- Ne sadrži informacije o vremenu po poteza

---

### 4.2 Podaci u realnom vremenu

- **Javni API-ji za dobijanje partija:**
  - [Chess.com Public API](https://www.chess.com/news/view/published-data-api)
  - [Lichess API - TV & Games](https://lichess.org/api#operation/streamTv)

- **Opis pristupa:**
  Planira se simulacija **real-time obrade** tako što će se dohvatati **gotove partije koje nisu uključene u istorijski dataset**, odnosno partije od **maja 2025. nadalje**.
  Time se omogućava praćenje **aktuelnih trendova i performansi igrača**, a podaci se mogu porediti sa istorijskim informacijama iz paketnog dataset-a.  
  Svaka nova partija će se periodično preuzimati i analizirati kako bi se posmatrali **promene u strategiji, popularnosti otvaranja i kvalitetu poteza** u odnosu na prethodne partije.

---

### 4.3 Napomena o PGN tagovima

Svi dataset-ovi sadrže **osnovni PGN format**, koji uključuje ključne tagove:

- `Event` – naziv događaja ili tip partije
- `Site` – mesto ili platforma gde je partija odigrana
- `UTCDate` / `UTCTime` – datum i vreme početka partije
- `WhiteElo` / `BlackElo` – rejting igrača
- `ECO` – šifra otvaranja po ECO klasifikaciji
- `Opening` – naziv šahovskog otvaranja
- `TimeControl` – kontrola vremena partije
- `Termination` – način završetka partije

### Dodatni tagovi po dataset-ovima
- **Lichess eval dataset:** `[%eval ...]`, `[%clk ...]`
- **Chess.com dataset:** `StartTime`, `EndTime`, `Link`, `CurrentPosition`
- **TWIC dataset:** `WhiteTitle`, `BlackTitle`, `WhiteFideId`, `BlackFideId`, `Round`, `Variation`


## 5. Pitanja za paketnu obradu podataka (Batch processing)

1. Profitabilnost otvaranja po Elo kategoriji

Koja su otvaranja koja donose najveći procenat pobeda, poraza ili remija igračima u različitim Elo kategorijama

2.Uticaj doba dana na iznadprosečne rezultate

U kojim delovima dana (jutro, dan, večе) igrač ostvaruje bolji procenat pobeda od svog globalnog prosečnog win rate-a?

3. Najuspešnije otvaranja po tipu partije

Koja otvaranja igrači koriste u različitim tipovima partija (bullet, blitz, rapid, classical) i koja imaju iznadprosečan win rate u odnosu na globalni prosečni win rate za taj tip partije?

4. Najčešća otvaranja igrača i uspešnost

Kojih je top 5 najčešće korišćenih otvaranja posmatranog igrača i koliki je procenat pobeda, poraza i remija za svako od njih?

5. Popularnost otvaranja u poslednjih 5 dana

Koja otvaranja su bila najčešće korišćena u poslednjih 5 dana i kako se raspodeljuju rezultati partija? 

6. Najveći ELO dobitak u prozoru partija

Koji je najveći kumulativni ELO dobitak igrača u prozoru od n partija?

7. Analiza performansi figura igrača

Koje figure daju igraču najbolji i najgori skor u partijama?

8. Loše serije figure igrača

Koje figure imaju najduže negativne serije (score ≤ 0) i kako te serije utiču na ishod partija?

9. Trend partija po stilu igre

Kako se igre igrača raspoređuju po stilovima igre (klasifikacijama: Divlje, Glatke, Tesne, Oštre, Uravnotežene) i kako stil utiče na ishod partija?

10. Najduži nizovi pobeda i poraza

Koji su najduži nizovi pobeda i najduži nizovi poraza za svakog igrača i kada su se oni dogodili?

11. Win/Lose streak (tilt analiza)

Da li igrač pokazuje tendenciju win-streaka ili lose-streaka, tj. da li pobeda ili poraz u prethodnoj partiji statistički utiče na ishod narednih partija?

12. Preokret iz loše pozicije

Koliko često igrač uspeva da pobedi partije u kojima je u sredini partije imao značajnu pozicionu manu (evaluacija < −0.5)?

13. Pobede uz materijalni deficit

Koliki je procenat partija u kojima igrač pobeđuje uprkos materijalnom deficitu, oslanjajući se na pozicionu ili inicijativnu kompenzaciju?

14. Tipologija partija

Kako su partije klasifikovane prema tipu (glatke, uravnotežene, oštre, tesne, iznenađujuće, divlje) i kakav je ishod u svakoj kategoriji?

15. Način završetka partija

Kako se raspodeljuju načini završetka partija koje je igrač pobedio, remizirao ili izgubio (šah-mat, predaja, istek vremena, napuštanje partije), i koji su najčešći razlozi za remi (trostruko ponavljanje pozicije, pat, pravilo 50 poteza, nedovoljno materijala, dogovoreni remi)?

---

## 6. Pitanja za obradu podataka u realnom vremenu (Stream processing)

1. Trenutni win/lose streak u poslednjih n dana

Da li je igrač trenutno u win-streaku ili lose-streaku?

2. Rokada i njen uticaj na ishod poslednjih n dana

Kako izvođenje rokade (kratke ili duge) utiče na procenat pobeda, poraza i remija igrača u poslednjih n partija ili u odabranom vremenskom periodu?

3. Uticaj šahova na ishod partije u poslednjih n dana

Kako broj odigranih šahova utiče na procenat pobeda igrača i da li određene figure ili faze partije donose veću uspešnost?

4. Najbolja sekvenca poteza u uživo partiji

Kako najbolja sekvenca poteza crnog igrača (bestSequence) utiče na tok partije i koliki je njen doprinos promeni evaluacije u partiji uživo?

5. Analiza kontrole polja u uživo partiji

Kako raspodela posećenih polja pokazuje strategiju kretanja figura igrača u uživo partiji? 

6. Kvalitet poteza po figurama u uživo partiji

Kako se kvalitet poteza razlikuje po tipu figure i koje figure najviše utiču na stabilnost partije u uživo partiji?

