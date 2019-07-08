from sklearn.preprocessing import StandardScaler, LabelEncoder


class MultiColumnLabelEncoder:
    def __init__(self, encoders = None, columns = None):
        self.columns = columns
        if encoders:
            self.encoders = encoders
        else:
            self.encoders = {}

    def fit(self,X,y=None):
        self.check_encoders = []
        return self

    def transform(self, X):
        '''
        Transforms columns of X specified in self.columns using
        LabelEncoder(). If no columns specified, transforms all
        columns in X.
        '''
        output = X.copy()
        if self.columns is not None:
            for col in self.columns:
                _le = LabelEncoder()
                if self.encoders.get(col):
                    output[col] = self.encoders.get(col).transform(output[col])
                else:
                    output[col] = _le.fit_transform(output[col])
                    self.encoders[col] = _le
                    self.check_encoders.append({col: _le})
        else:
            for colname,col in output.iteritems():
                _le = LabelEncoder()
                if self.encoders.get(col):
                    output[colname] = self.encoders.get(col).transform(col)
                else:
                    output[colname] = _le.fit_transform(col)
                    self.encoders[col] = _le
                    self.check_encoders.append({col: _le})
        return output

    def fit_transform(self,X,y=None):
        return self.fit(X,y).transform(X)