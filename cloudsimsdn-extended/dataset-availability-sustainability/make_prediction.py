import sys
import pickle
import pandas as pd

def make_prediction(feature_file):
    try:
        model_path = "dataset-availability-sustainability/models/Gradient Boosting.pickle"
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
            
        df = pd.read_csv(feature_file)
        
        platforms = {'A': 0, 'B': 1, 'C': 2, 'D': 3}
        if 'platform_id' in df.columns:
            df['platform_id_encoded'] = df['platform_id'].map(lambda x: platforms.get(x, 0))
            
        clusters = {f'host{i}': i for i in range(100)}
        if 'cluster' in df.columns:
            df['cluster_encoded'] = df['cluster'].map(lambda x: clusters.get(x, 0))
        
        features_to_use = ['cpus', 'memory', 'platform_id_encoded', 'cluster_encoded', 'time']
        features_available = [f for f in features_to_use if f in df.columns]
        
        if len(features_available) > 0:
            prediction = model.predict(df[features_available])[0]
            print(prediction)
            return prediction
        else:
            print(0)
            return 0
            
    except Exception as e:
        print(f"Error during prediction: {e}", file=sys.stderr)
        print(0)
        return 0

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: make_prediction.py <feature_csv_file>", file=sys.stderr)
        sys.exit(1)
        
    feature_file = sys.argv[1]
    make_prediction(feature_file)