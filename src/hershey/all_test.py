import unittest 

if __name__=='__main__':
    suite=unittest.TestLoader().loadTestsFromNames([
        'feed_test',
        'npb_test',
        'mlb_test',
    ])
    unittest.TextTestRunner(verbosity=2).run(suite)
