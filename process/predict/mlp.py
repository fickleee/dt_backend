import torch

class MLP(torch.nn.Module):
    def __init__(self, input_size=3, hidden_size1=32, hidden_size2=64, hidden_size3=32, output_size=1):
        super(MLP, self).__init__()
        self.fc1 = torch.nn.Linear(input_size, hidden_size2)
        self.fc3 = torch.nn.Linear(hidden_size2, hidden_size3)
        self.fc4 = torch.nn.Linear(hidden_size3, output_size)
        self.relu = torch.nn.ReLU()

    def forward(self, x):
        x = self.relu(self.fc1(x))
        x = self.relu(self.fc3(x))
        x = self.fc4(x)
        return x