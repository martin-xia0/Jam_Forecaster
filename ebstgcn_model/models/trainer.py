from data_loader.data_utils import gen_batch
from models.tester import model_inference
from models.base_model import build_model, model_save
from os.path import join as pjoin

import tensorflow as tf
import numpy as np
import time


def model_train(inputs, blocks, args, sum_path='./output/tensorboard'):
    '''
    Train the base model.
    :param inputs: instance of class Dataset, data source for training.
    :param blocks: list, channel configs of st_conv blocks.
    :param args: instance of class argparse, args for training.
    '''
    n, n_his, n_pred = args.n_route, args.n_his, args.n_pred
    Ks, Kt = args.ks, args.kt
    batch_size, epoch, inf_mode, opt = args.batch_size, args.epoch, args.inf_mode, args.opt

    # Placeholder for model training
    x = tf.placeholder(tf.float32, [None, n_his + 1, n, 1], name='data_input')
    keep_prob = tf.placeholder(tf.float32, name='keep_prob')

    # Define model loss
    train_loss, pred = build_model(x, n_his, Ks, Kt, blocks, keep_prob)
    tf.summary.scalar('train_loss', train_loss)
    copy_loss = tf.add_n(tf.get_collection('copy_loss'))
    tf.summary.scalar('copy_loss', copy_loss)

    # Learning rate settings
    global_steps = tf.Variable(0, trainable=False)
    len_train = inputs.get_len('train')
    if len_train % batch_size == 0:
        epoch_step = len_train / batch_size
    else:
        epoch_step = int(len_train / batch_size) + 1
    # Learning rate decay with rate 0.7 every 5 epochs.
    lr = tf.train.exponential_decay(args.lr, global_steps, decay_steps=5 * epoch_step, decay_rate=0.7, staircase=True)
    tf.summary.scalar('learning_rate', lr)
    step_op = tf.assign_add(global_steps, 1)
    with tf.control_dependencies([step_op]):
        if opt == 'RMSProp':
            train_op = tf.train.RMSPropOptimizer(lr).minimize(train_loss)
        elif opt == 'ADAM':
            train_op = tf.train.AdamOptimizer(lr).minimize(train_loss)
        else:
            raise ValueError('ERROR: optimizer "{}" is not defined.'.format(opt))

    merged = tf.summary.merge_all()

    with tf.Session() as sess:
        writer = tf.summary.FileWriter(pjoin(sum_path, 'train'), sess.graph)
        sess.run(tf.global_variables_initializer())

        if inf_mode == 'sep':
            # for inference mode 'sep', the type of step index is int.
            step_idx = n_pred - 1
            tmp_idx = [step_idx]
            min_val = min_va_val = np.array([4e1, 1e5, 1e5])
        elif inf_mode == 'merge':
            # for inference mode 'merge', the type of step index is np.ndarray.
            step_idx = tmp_idx = np.arange(3, n_pred + 1, 3) - 1
            min_val = min_va_val = np.array([4e1, 1e5, 1e5] * len(step_idx))
        else:
            raise ValueError('ERROR: test mode "{}" is not defined.'.format(inf_mode))

        for i in range(epoch):
            start_time = time.time()
            for j, x_batch in enumerate(
                    gen_batch(inputs.get_data('train'), batch_size, dynamic_batch=True, shuffle=True)):
                summary, _ = sess.run([merged, train_op], feed_dict={x: x_batch[:, 0:n_his + 1, :, :], keep_prob: 1.0})
                writer.add_summary(summary, i * epoch_step + j)
                if j % 50 == 0:
                    loss_value = \
                        sess.run([train_loss, copy_loss],
                                 feed_dict={x: x_batch[:, 0:n_his + 1, :, :], keep_prob: 1.0})
                    print('Epoch {:2d}, Step {:3d}: [{:.3f}, {:.3f}]'.format(i, j, loss_value[0], loss_value[1]))
            print('Epoch {:2d} Training Time {:.3f}s'.format(i, time.time() - start_time))

            start_time = time.time()
            min_va_val, min_val = \
                model_inference(sess, pred, inputs, batch_size, n_his, n_pred, step_idx, min_va_val, min_val)

            for ix in tmp_idx:
                va, te = min_va_val[ix - 2:ix + 1], min_val[ix - 2:ix + 1]
                print('Time Step {}: '.format(ix + 1),
                      'MAPE {:7.3%}, {:7.3%}; '.format(va[0], te[0]),
                      'MAE  {:4.3f}, {:4.3f}; '.format(va[1], te[1]),
                      'RMSE {:6.3f}, {:6.3f}.'.format(va[2], te[2]))
            print('Epoch {:2d} Inference Time {:.3f}s'.format(i, time.time() - start_time))

            if (i + 1) % args.save == 0:
                model_save(sess, global_steps, 'STGCN')
        writer.close()
    print('Training model finished!')
