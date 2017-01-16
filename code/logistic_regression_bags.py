import graphlab as gl
import numpy as np
from sklearn.metrics import roc_auc_score

import utils

# put in the path to the kaggle data
PATH_TO_JSON = "../data/json8/"
PATH_TO_TRAIN_LABELS = "../data/train_v2.csv"
PATH_TO_TEST_LABELS = "../data/sampleSubmission_v2.csv"

MODE = 'sub'  # (exp)eriment|(sub)mission


# read json blocks from path PATH_TO_JSON
sf = gl.SFrame.read_csv(PATH_TO_JSON, header=False)
sf = sf.unpack('X1', column_name_prefix='')

# read train labels from paths PATH_TO_TRAIN_LABELS and PATH_TO_TEST_LABELS
train_labels = gl.SFrame.read_csv(PATH_TO_TRAIN_LABELS)
# create a new columns "id" from parsing urlId and drop file columns
train_labels['id'] = train_labels['file'].apply(lambda x: str(x.split('_')[0]))
train_labels = train_labels.remove_column('file')
# join labels with html data from training and testing SFrames
train = train_labels.join(sf, on='id', how='left')

train = utils.process_dataframe(train)

# bot (Bag of Tags) NGRAM
train['bot'] = gl.text_analytics.count_words(train['tags'])
train['bol'] = gl.text_analytics.count_words(train['text_links'])
train['boi'] = gl.text_analytics.count_words(train['text_img'])
train = train.dropna()

if MODE == 'exp':
    train, validation = train.random_split(0.95, seed=0)
elif MODE == 'sub':
    test_labels = gl.SFrame.read_csv(PATH_TO_TEST_LABELS)
    test_labels['id'] = test_labels['file'].apply(lambda x: str(x.split('_')[0]))
    test_labels = test_labels.remove_column('file')
    test = test_labels.join(sf, on='id', how='left')
    test = utils.process_dataframe(test)
    # bot (Bag of Tags) NGRAM
    test['bot'] = gl.text_analytics.count_words(test['tags'])
    test['bol'] = gl.text_analytics.count_words(test['text_links'])
    test['boi'] = gl.text_analytics.count_words(test['text_img'])
    test = test.dropna()

features = ['num_images', 'num_links', 'num_tags', 'bot', 'bol', 'boi']

model = gl.logistic_classifier.create(train, target='sponsored', features=features, class_weights='auto', max_iterations=1, convergence_threshold=0.0001, l2_penalty=0.0, l1_penalty=0.999, validation_set=None)

if MODE == 'exp':
    ypred = model.predict(validation, output_type='probability')
    y = validation['sponsored']
    print 'AUC', roc_auc_score(np.array(y), np.array(ypred))
elif MODE == 'sub':
    train = None
    ypred = model.predict(test, output_type='probability')

    # create submission.csv
    submission = gl.SFrame()
    submission['sponsored'] = ypred
    submission['file'] = test['id'].apply(lambda x: x + '_raw_html.txt')
    submission.save('output/submission_lr_bags.csv', format='csv')
