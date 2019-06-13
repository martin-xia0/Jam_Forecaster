import os
import numpy as np
import datetime as dt
import json
import pandas as pd
from keras.layers import Dense, Activation, Dropout, LSTM
from keras.models import Sequential, load_model
from keras.callbacks import EarlyStopping, ModelCheckpoint


class Model:
    def __init__(self, config_file):
        self.model = Sequential()
        with open(config_file, encoding='utf-8') as f:
            self.config = json.load(f)
        print("create model {}".format(self.config))
        self.epochs = self.config["epochs"] 
        self.batch_size = self.config["batch_size"] 
        self.steps_per_epoch = self.config["steps_per_epoch"]

    def load_model(self, file_name):
        print("load model {}".format(file_name))
        self.model = load_model(file_name)

    def load_data(self):
        interval = 15
        df = pd.read_csv("./data.csv")
        data = df.values
        end = len(df) - interval * 2 - 1
        print("Finish data loading, df length {}".format(len(df)))
        train_x = []
        train_y = []
        for i in range(end):
            row_x = data[i: i+interval]
            x = np.array([i[1:] for i in row_x])
            y = np.array(data[i+interval*2-1][1:])
            train_x.append(x)
            train_y.append(y)
        train_x = np.array(train_x)
        train_y = np.array(train_y)
        print("数据集大小x{}，y{}".format(train_x.shape, train_y.shape))
        return train_x, train_y

    def build_model(self):
        print("start building model")
        for layer in self.config['layers']:
            neurons = layer['neurons'] if 'neurons' in layer else None
            dropout_rate = layer['rate'] if 'rate' in layer else None
            activation = layer['activation'] if 'activation' in layer else None
            return_seq = layer['return_seq'] if 'return_seq' in layer else None
            input_timesteps = layer['input_timesteps'] if 'input_timesteps' in layer else None
            input_dim = layer['input_dim'] if 'input_dim' in layer else None

            if layer['type'] == 'dense':
                self.model.add(Dense(units=neurons, activation=activation))
            if layer['type'] == 'lstm':
                self.model.add(LSTM(units=neurons, input_shape=(input_timesteps, input_dim), return_sequences=return_seq))
            if layer['type'] == 'dropout':
                self.model.add(Dropout(dropout_ratex))

        self.model.compile(loss=self.config['loss'], optimizer=self.config['optimizer'])
        print("Model compiled")

    def train_generator(self, data_gen, save_dir):
        print("start training model")
        print('Model %s epochs, %s batch size, %s batches per epoch' % (self.epochs,self.batch_size,self.steps_per_epoch))
        file_name = os.path.join(save_dir, '%s-e%s.h5' % (dt.datetime.now().strftime('%d%m%Y-%H%M%S'), str(self.epochs)))
        callbacks = [
            ModelCheckpoint(filepath=file_name, monitor='loss', save_best_only=True)
        ]
        self.model.fit_generator(
            data_gen,
            steps_per_epoch=self.steps_per_epoch,
            epochs=self.epochs,
            callbacks=callbacks,
            workers=1
        )
        print('[Model] Training Completed. Model saved as %s' % file_name)

    def train(self, x, y, save_dir):
        print("start training model")
        print('Model %s epochs, %s batch size, %s batches per epoch' % (self.epochs,self.batch_size,self.steps_per_epoch))
        file_name = os.path.join(save_dir, '%s-e%s.h5' % (dt.datetime.now().strftime('%d%m%Y-%H%M%S'), str(self.epochs)))
        callbacks = [
            ModelCheckpoint(filepath=file_name, monitor='loss', save_best_only=True)
        ]
        self.model.fit(
            x=x,
            y=y,
            batch_size=self.batch_size,
            epochs=self.epochs,
            callbacks=callbacks,
        )
        print('[Model] Training Completed. Model saved as %s' % file_name)

    def test(self, x, y):
        print("start testing model")
        predict_y = self.model.predict(x, self.batch_size, verbose=1).flatten()
        n = predict_y.shape[0]
        y = np.array([float(x) for x in y])
        print(predict_y)
        print(y)
        # MAPE：平均绝对百分误差
        MAPE = sum(abs((predict_y-y) / y)) / n
        # MAE：平均绝对误差
        MAE = sum(abs(predict_y-y)) / n
        # RMSE：均方根误差
        RMSE = (sum(predict_y**2-y**2) / n)**0.5
        print("MAE {}, MAPE {}, RMSE {}".format(MAE, MAPE, RMSE))


if __name__ == '__main__':

    # -------模式1：训练模型+测试-------#
    # 新建模型
    lstm = Model("config.json")
    lstm.build_model()
    # 加载数据
    train_x, train_y = lstm.load_data()
    # 训练
    lstm.train(train_x, train_y, "hist_models")

    # -------模式2：加载模型+测试-------#
    '''
    # 加载已有模型
    lstm = Model("config.json")
    lstm.load_model("predictor.h5")
    # 加载数据
    test_x = np.load("test_x.npy")
    test_y = np.load("test_y.npy")
    # 测试
    lstm.test(test_x, test_y)
    '''