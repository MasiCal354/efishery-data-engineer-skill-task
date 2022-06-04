# Data Transfromation Pipeline

## Setup

1. Install Python and requirements on `requirements.txt`
2. Go to `transformation` as active directory
3. execute `. run.sh`

Output:

```bash
1. lele: 2459kg
2. mas: 1541kg
3. nila: 1049kg
4. tongkol: 946kg
5. mujaer: 487kg
6. bawal: 412kg
7. kakap: 375kg
8. gurame: 353kg
9. kembung: 349kg
10. laut: 115kg
11. kerapu: 105kg
12. patin: 85kg
13. salem: 52kg
14. bandeng: 42kg
15. merah: 36kg
16. udang: 27kg
17. cumi: 20kg
```

## Adjusting Config

Open `config.py` to adjust `MIN_KOMODITAS_COUNT_QUANTILE` and `MIN_KOMODITAS_MATCHING_RATIO`.

## Git Log

```log
57535960e2e7afb7ac85d10dac82bd208e791550 tidy up requirements
ef98b1e08eeff6d5535058124fc5eaf6a31f3b09 implement on run.sh
9fe79c6a5d5883b6b26c8a4ba4a01b2c1801a6e2 run pre-commit
26cc420e9c12bef2529fa6b079435f64afadf449 tidy up
7dc8fc9968ec56fb16e80fecce41aed51f6e8023 implement solution for question 2
afbd24d995f2d48a599978af0f451398a0c05693 solve question 1
2d79d26df5aba04ebd187e86b3b95a0612032b99 Initial commit
```

## Reasoning

Komoditas cleansng on this implementation is more like statistical data cleansing by implementing word count on each word and use them as a basis on either keeping the word or replacing them based on similarity. For berat cleansing, I want to keep numbers only and ordering the in such a way if the number of berat similar to number of komoditas we can map them in order, else we can pick the first value or None if there is no number available on the berat. This implementation can be improved as it might ignore some corner cases like berat without number but actually exist like sekilo or setengah kilo, some keywords like kecuali can also be implemented. Those aren't implemented because of time limitation but the author is aware of the case.
