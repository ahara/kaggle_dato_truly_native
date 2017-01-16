# Kaggle competition Dato: Truly Native?
### Dato Creators Award winning solution
https://www.kaggle.com/c/dato-native

## Steps to build model (in Linux):

1. Expected directories structure:

    ```
        + <parent directory>
        |---+ data
        |   |--- <unpacked data from kaggle>
        |
        |---+ code (working directory)
            |--- <python files provided in code directory in this repository>
    ```
2. Assuming that data are unpacked in `data` folder, run:

    ```
    mkdir ../data/json8
    python process_html.py ../data/ ../data/json8
    ```
   to prepare JSONs with data.
3. Final model is an ensemble of boosted trees and logistic classifiers,
   but the most predictive power is from the first one. Logistic classifiers
   provide only very small improvement.
4. Build five logistic classifiers. One for each chunk of data:
	
	```
	mkdir models
	python chunk_classifier.py
	```
5. Build logistic classifier model on all data:

	```
	mkdir output
	python logistic_regression_bags.py
	```
6. Combine all logistic models:

	```
	python combine_lr.py
	```
7. Build boosted trees model:

	```
	python gbt2.py
	```
   Building this model requires around 90GB of memory, but it can also be
   performed efficiently on machine with 16GB of RAM if big enough swap file
   will be created - it will allow to allocate GLC required memory, but still
   paging won't be an issue as effectively it uses only 10 GB.
8. Ensemble boosted trees model and combined logistic classifiers:

	```
	python ensembler.py
	```
9. Final submission should be in `output/ensembler.csv` file.
10. As a version 1.5.2 of graphlab doesn't support random_seed parameter for
    boosted trees classifier the results can be slightly different between
    runs.
