import unittest

if __name__=='__main__':
    suite=unittest.TestLoader().loadTestsFromNames([
        'kbo_test',
        'npb_test',
        'mlb_test',
        'kl_test',
        'delta_test',
    ])
    unittest.TextTestRunner(verbosity=2).run(suite)
