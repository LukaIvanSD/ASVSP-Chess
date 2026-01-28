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

1. Prosečna dužina partije

Koji je prosečan broj poteza po partiji za posmatranog igrača i kako se on razlikuje u zavisnosti od tempa igre (bullet, blitz, rapid, classical)?

2. Uticaj doba dana na ishod partije

Kako doba dana (jutro, popodne, veče, noć) utiče na procenat pobeda, poraza i remija igrača?

3. Promena popularnosti otvaranja kroz vreme

Kako se popularnost određenih šahovskih otvaranja menjala kroz godine na globalnom nivou i kod posmatranog igrača?

4. Najčešća otvaranja igrača i uspešnost

Kojih je top 5 najčešće korišćenih otvaranja posmatranog igrača i koliki je procenat pobeda, poraza i remija za svako od njih?

5. Uticaj niskog vremena na ishod

U kom procentu partija igrač gubi kada mu preostane manje od određenog broja sekundi (npr. <10s, <5s) i koliko često uspeva da preživi takve situacije?

6. Preferencije u razmeni figura

Da li igrač u završnici češće zadržava par lovaca ili par konja, i kako ta preferencija utiče na ishod partije?

7. Realizacija mat pretnji

U kojem procentu slučajeva igrač uspešno realizuje pozicije u kojima engine evaluira forsirani mat u nekoliko poteza, i koliki je procenat “izgubljenih mat šansi”?

8. Uspešnost otvaranja prema evaluaciji

Koja otvaranja najčešće dovode do stabilne prednosti (≥ +0.5) za belog ili crnog u prvih 10 poteza?

9. Rokada i njen uticaj na ishod

Koliko često igrač radi malu, veliku ili uopšte ne radi rokadu, u kojoj fazi partije (rana, srednja, kasna), i kako te odluke utiču na procenat pobeda?

10. Kritične greške tokom partije

Koliki procenat kritičnih grešaka (pad evaluacije >1 poen) igrač pravi u prvoj polovini partije u odnosu na drugu polovinu?

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

1. Trenutni win/lose streak

Da li je igrač trenutno u win-streaku ili lose-streaku i kako se to razlikuje od njegovog istorijskog proseka?

2. Promena repertoara otvaranja

Koja su otvaranja u poslednjih nekoliko partija postala značajno češća u odnosu na raniji period?

3. Realizacija prednosti u novijem periodu

Da li igrač u poslednjih nekoliko partija češće ili ređe uspeva da realizuje pozicionu prednost u odnosu na istorijske podatke?

4. Vreme igranja u skorije vreme

Kako doba dana i dan u nedelji utiču na rezultate igrača u poslednjih nekoliko partija u poređenju sa istorijom?

5. Kvalitet poteza u skorije vreme

Kako se raspodela kvaliteta poteza (najbolji, odličan, dobar, slab, greška) promenila u poslednjih nekoliko partija?

6. Aktivnost figura po fazama igre

Kako se promenio broj poteza sa pojedinim figurama (kraljica, topovi, laki oficiri) u otvaranju, sredini i završnici u poslednjem periodu?

7. Kritične greške

Da li se raspodela kritičnih grešaka po fazama partije u poslednjih nekoliko partija razlikuje u odnosu na istorijske podatke?

8. Pronađeni matovi

Kako se broj pronađenih matova u 1, 2, 3, 4 i 5+ poteza u poslednjih nekoliko partija poredi sa istorijskim periodom?

9. Ishod partija

Da li se način završetka partija (predaja, istek vremena, šah-mat) u poslednjih nekoliko partija razlikuje u odnosu na istorijske obrasce?

