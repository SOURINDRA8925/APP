import pickle
import os

print("📦 INITIALIZING PICKLE BINARY GENERATOR MATRIX...")

# Rule 1 Dummy Classifier: Triggers if Transaction Amount > 50000
class AmountRuleModel:
    def predict(self, features):
        # features list structure: [[amount, channel_code]]
        amount = features[0][0]
        return 1 if amount > 50000 else 0

# Rule 2 Dummy Classifier: Triggers if Amount > 10000 AND Channel is ATM (code == 1)
class ChannelRuleModel:
    def predict(self, features):
        amount = features[0][0]
        channel_code = features[0][1]
        return 1 if (channel_code == 1 and amount > 10000) else 0

# Rule 3 Dummy Classifier: Simple wildcard anomaly detector pass-through
class NarrationRuleModel:
    def predict(self, features):
        return 0  # Fallback pass-through gate

# Compile and serialize the 3 binary rule models to your folder tree
models = {
    'rule1_amount_model.pkl': AmountRuleModel(),
    'rule2_channel_model.pkl': ChannelRuleModel(),
    'rule3_narration_model.pkl': NarrationRuleModel()
}

for filename, model_obj in models.items():
    with open(filename, 'wb') as f:
        pickle.dump(model_obj, f)
    print(f"✅ SUCCESSFULLY BUILT COMPILED ASSET: '{filename}'")

print("\n🎉 Your 3 pickle files are live inside your root project folder directory!")
