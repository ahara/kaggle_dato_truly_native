import graphlab as gl
import numpy as np
import pdb


def proba_to_posprob(p):
    n = len(p)
    posprob = np.array([0.] * n)
    position = np.argsort(p)
    for i, pos in enumerate(position):
        posprob[pos] = i / float(n - 1)
    return posprob


if __name__ == '__main__':
    gbt = gl.SFrame.read_csv('output/submission_gbt2.csv', header=True)
    lr = gl.SFrame.read_csv('output/submission_chunks_01234_lr.csv', header=True)
    plr = lr['sponsored']
    pgbt = gbt['sponsored']

    s = gl.SFrame()
    s['file'] = gbt['file']
    s['sponsored'] = .1 * proba_to_posprob(plr) + .9 * proba_to_posprob(pgbt)
    s.save('output/ensembler.csv', format='csv')
