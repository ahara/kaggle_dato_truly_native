import graphlab as gl
import numpy as np
import os

import utils

# put in the path to the kaggle data
PATH_TO_JSON = "../data/json8/"
PATH_TO_TRAIN_LABELS = "../data/train_v2.csv"
PATH_TO_TEST_LABELS = "../data/sampleSubmission_v2.csv"

for b in [0, 1, 2, 3, 4]:
    # read json blocks from path PATH_TO_JSON
    sf = gl.SFrame.read_csv(os.path.join(PATH_TO_JSON, 'chunk%d.json' % b), header=False)
    sf = sf.unpack('X1', column_name_prefix='')

    # read train labels from paths PATH_TO_TRAIN_LABELS and PATH_TO_TEST_LABELS
    train_labels = gl.SFrame.read_csv(PATH_TO_TRAIN_LABELS)
    # create a new columns "id" from parsing urlId and drop file columns
    train_labels['id'] = train_labels['file'].apply(lambda x: str(x.split('_')[0]))
    train_labels = train_labels.remove_column('file')
    # join labels with html data from training and testing SFrames
    train = train_labels.join(sf, on='id', how='left')

    train = utils.process_dataframe(train)

    # NGRAMs
    train['links_ngram'] = gl.text_analytics.count_ngrams(train['text_links'], 3)
    train['img_ngram'] = gl.text_analytics.count_ngrams(train['text_img'], 3)
    train = train.dropna()

    features = ['num_images', 'num_links', 'links_ngram', 'img_ngram']

    model = gl.logistic_classifier.create(train, target='sponsored', features=features, class_weights='auto', max_iterations=2, convergence_threshold=0.0001, l2_penalty=0.0, l1_penalty=0.999, validation_set=None, step_size=.6)
    model.save('models/model%d' % b)
