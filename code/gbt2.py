import graphlab as gl
import numpy as np
import pdb
import re
from graphlab.toolkits.feature_engineering import FeatureHasher
from sklearn.metrics import roc_auc_score

import utils


# put in the path to the kaggle data
PATH_TO_JSON = "../data/json8/"
PATH_TO_TRAIN_LABELS = "../data/train_v2.csv"
PATH_TO_TEST_LABELS = "../data/sampleSubmission_v2.csv"

MODE = 'sub'  # (exp)eriment|(sub)mission|tune


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

# BOWs
train['bow_title'] = gl.text_analytics.count_words(train['title'])
train['bow_title'] = train['bow_title'].dict_trim_by_keys(gl.text_analytics.stopwords(), True)
train['bow_header'] = gl.text_analytics.count_words(train['header'])
train['bow_header'] = train['bow_header'].dict_trim_by_keys(gl.text_analytics.stopwords(), True)
train['bow_lists'] = gl.text_analytics.count_words(train['lists'])
train['bow_lists'] = train['bow_lists'].dict_trim_by_keys(gl.text_analytics.stopwords(), True)
train['bow_meta'] = gl.text_analytics.count_words(train['meta'])
train['bow_meta'] = train['bow_meta'].dict_trim_by_keys(gl.text_analytics.stopwords(), True)
train['bow_meta_property'] = gl.text_analytics.count_words(train['meta_property'])
train['bow_meta_name'] = gl.text_analytics.count_words(train['meta_name'])
train['bow_links_visible_text'] = gl.text_analytics.count_words(train['links_visible_text'])
train['bow_links_visible_text'] = train['bow_links_visible_text'].dict_trim_by_keys(gl.text_analytics.stopwords(), True)
train['bolp'] = gl.text_analytics.count_words(train['links_parents'])
train['bot'] = gl.text_analytics.count_words(train['tags'])
train['boa'] = gl.text_analytics.count_words(train['attr'])
train['bol'] = gl.text_analytics.count_words(train['text_links'])
train['bol'] = train['bol'].dict_trim_by_keys(gl.text_analytics.stopwords(), True)
train['boi'] = gl.text_analytics.count_words(train['text_img'])
train['boi'] = train['boi'].dict_trim_by_keys(gl.text_analytics.stopwords(), True)
train = train.dropna()


def eliminate_rare_keys(train_set, test_set, field):
    print 'Eliminating keys for', field
    keys = test_set.flat_map([field], lambda x: map(lambda xx: [xx], x[field].keys())).unique()
    return train_set[field].dict_trim_by_keys(keys[field], False)


if MODE == 'exp':
    train, validation = train.random_split(0.95, seed=0)

    train['bot'] = eliminate_rare_keys(train, validation, 'bot')
    train['boa'] = eliminate_rare_keys(train, validation, 'boa')
    train['bol'] = eliminate_rare_keys(train, validation, 'bol')
    train['boi'] = eliminate_rare_keys(train, validation, 'boi')

    train['bow_title'] = eliminate_rare_keys(train, validation, 'bow_title')
    train['bow_header'] = eliminate_rare_keys(train, validation, 'bow_header')
    train['bow_lists'] = eliminate_rare_keys(train, validation, 'bow_lists')
    train['bow_meta'] = eliminate_rare_keys(train, validation, 'bow_meta')
    train['bow_meta_name'] = eliminate_rare_keys(train, validation, 'bow_meta_name')
    train['bow_meta_property'] = eliminate_rare_keys(train, validation, 'bow_meta_property')
    train['bow_links_visible_text'] = eliminate_rare_keys(train, validation, 'bow_links_visible_text')

    print 'Start hashing'
    hasher_img = gl.feature_engineering.create(train, FeatureHasher(features='boi', num_bits=13, output_column_name='hboi'))
    train = hasher_img.transform(train)
    validation = hasher_img.transform(validation)

    hasher_link = gl.feature_engineering.create(train, FeatureHasher(features='bol', num_bits=12, output_column_name='hbol'))
    train = hasher_link.transform(train)
    validation = hasher_link.transform(validation)

    hasher_lists = gl.feature_engineering.create(train, FeatureHasher(features='bow_lists', num_bits=11, output_column_name='hbow_lists'))
    train = hasher_lists.transform(train)
    validation = hasher_lists.transform(validation)

    hasher_lvt = gl.feature_engineering.create(train, FeatureHasher(features='bow_links_visible_text', num_bits=11, output_column_name='hbow_lvt'))
    train = hasher_lvt.transform(train)
    validation = hasher_lvt.transform(validation)

elif MODE == 'sub':
    test_labels = gl.SFrame.read_csv(PATH_TO_TEST_LABELS)
    test_labels['id'] = test_labels['file'].apply(lambda x: str(x.split('_')[0]))
    test_labels = test_labels.remove_column('file')
    test = test_labels.join(sf, on='id', how='left')
    test = utils.process_dataframe(test)

    test['bow_title'] = gl.text_analytics.count_words(test['title'])
    test['bow_title'] = test['bow_title'].dict_trim_by_keys(gl.text_analytics.stopwords(), True)
    test['bow_header'] = gl.text_analytics.count_words(test['header'])
    test['bow_header'] = test['bow_header'].dict_trim_by_keys(gl.text_analytics.stopwords(), True)
    test['bow_lists'] = gl.text_analytics.count_words(test['lists'])
    test['bow_lists'] = test['bow_lists'].dict_trim_by_keys(gl.text_analytics.stopwords(), True)
    test['bow_meta'] = gl.text_analytics.count_words(test['meta'])
    test['bow_meta'] = test['bow_meta'].dict_trim_by_keys(gl.text_analytics.stopwords(), True)
    test['bow_meta_property'] = gl.text_analytics.count_words(test['meta_property'])
    test['bow_meta_name'] = gl.text_analytics.count_words(test['meta_name'])
    test['bow_links_visible_text'] = gl.text_analytics.count_words(test['links_visible_text'])
    test['bow_links_visible_text'] = test['bow_links_visible_text'].dict_trim_by_keys(gl.text_analytics.stopwords(), True)


    test['bot'] = gl.text_analytics.count_words(test['tags'])
    test['boa'] = gl.text_analytics.count_words(test['attr'])
    test['bol'] = gl.text_analytics.count_words(test['text_links'])
    test['bol'] = test['bol'].dict_trim_by_keys(gl.text_analytics.stopwords(), True)
    test['boi'] = gl.text_analytics.count_words(test['text_img'])
    test['boi'] = test['boi'].dict_trim_by_keys(gl.text_analytics.stopwords(), True)
    test['bolp'] = gl.text_analytics.count_words(test['links_parents'])

    test = test.dropna()

    train['bot'] = eliminate_rare_keys(train, test, 'bot')
    train['boa'] = eliminate_rare_keys(train, test, 'boa')
    train['bol'] = eliminate_rare_keys(train, test, 'bol')
    train['boi'] = eliminate_rare_keys(train, test, 'boi')

    train['bow_title'] = eliminate_rare_keys(train, test, 'bow_title')
    train['bow_header'] = eliminate_rare_keys(train, test, 'bow_header')
    train['bow_lists'] = eliminate_rare_keys(train, test, 'bow_lists')
    train['bow_meta'] = eliminate_rare_keys(train, test, 'bow_meta')
    train['bow_meta_name'] = eliminate_rare_keys(train, test, 'bow_meta_name')
    train['bow_meta_property'] = eliminate_rare_keys(train, test, 'bow_meta_property')
    train['bow_links_visible_text'] = eliminate_rare_keys(train, test, 'bow_links_visible_text')

    hasher_img = gl.feature_engineering.create(train, FeatureHasher(features='boi', num_bits=13, output_column_name='hboi'))
    train = hasher_img.transform(train)
    test = hasher_img.transform(test)

    hasher_link = gl.feature_engineering.create(train, FeatureHasher(features='bol', num_bits=12, output_column_name='hbol'))
    train = hasher_link.transform(train)
    test = hasher_link.transform(test)

    hasher_lists = gl.feature_engineering.create(train, FeatureHasher(features='bow_lists', num_bits=11, output_column_name='hbow_lists'))
    train = hasher_lists.transform(train)
    test = hasher_lists.transform(test)

    hasher_lvt = gl.feature_engineering.create(train, FeatureHasher(features='bow_links_visible_text', num_bits=11, output_column_name='hbow_lvt'))
    train = hasher_lvt.transform(train)
    test = hasher_lvt.transform(test)

features = ['bow_title', 'bow_header', 'hbow_lists', 'bow_meta', 'bow_meta_name', 'bow_meta_property', 'hbow_lvt', 'bolp', 'num_images', 'num_links', 'num_tags', 'bot', 'hbol', 'hboi', 'num_attr']
features += [n for n in train.column_names() if re.match('^[sabcd]_', n)]

model = gl.boosted_trees_classifier.create(train, target='sponsored', features=features, max_iterations=3200, max_depth=25, min_child_weight=.9, row_subsample=0.5, min_loss_reduction=.75, column_subsample=0.45, step_size=.0085, verbose=True, validation_set=None, class_weights='auto')

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
    submission.save('output/submission_gbt2.csv', format='csv')
