import graphlab as gl
import numpy as np
import os
from sklearn.metrics import roc_auc_score

import utils

# put in the path to the kaggle data
PATH_TO_JSON = "../data/json8/"
PATH_TO_TRAIN_LABELS = "../data/train_v2.csv"
PATH_TO_TEST_LABELS = "../data/sampleSubmission_v2.csv"

predictions = []

for b in [5]:
    # read json blocks from path PATH_TO_JSON
    sf = gl.SFrame.read_csv(os.path.join(PATH_TO_JSON, 'chunk%d.json' % b), header=False)
    sf = sf.unpack('X1', column_name_prefix='')

    test_labels = gl.SFrame.read_csv(PATH_TO_TEST_LABELS)
    test_labels['id'] = test_labels['file'].apply(lambda x: str(x.split('_')[0]))
    test_labels = test_labels.remove_column('file')
    test = test_labels.join(sf, on='id', how='left')
    test = utils.process_dataframe(test)

    test['links_ngram'] = gl.text_analytics.count_ngrams(test['text_links'], 3)
    test['img_ngram'] = gl.text_analytics.count_ngrams(test['text_img'], 3)
    test = test.dropna()

ypred = None
for b in [0, 1, 2, 3, 4]:
    print 'Loading model', b
    model = gl.load_model('models/model%s' % str(b))
    ypred = model.predict(test, output_type='probability')
    predictions.append(ypred)
for p in predictions:
    ypred = p if ypred is None else ypred + p
lr = gl.SFrame.read_csv('output/submission_lr_bags.csv')
ypred += lr['sponsored']
# create submission file
submission = gl.SFrame()
submission['sponsored'] = ypred / 6.
submission['file'] = test['id'].apply(lambda x: x + '_raw_html.txt')
submission.save('output/submission_chunks_01234_lr.csv', format='csv')
