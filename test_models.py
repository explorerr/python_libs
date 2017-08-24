import unittest
import models as mdl
import numpy as np

class TestModels(unittest.TestCase):
    
    def test_cvrmse(self):
        pred = np.arange(10)
        actu = np.arange(10,20)
        self.assertEqual(mdl.cvrmse(pred, actu), 0.68965517241379315)
        
if __name__ == '__main__':
    unittest.main()
        