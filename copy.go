package main

//cp h:user:%d|z:bk:@hrtBid:st:%d:pks z:user:%0:hrtpks:by:lst
func Copy(xdb *XDB) (count int, err error) {
	c, err := pool.NewClient()
	if err != nil {
		return
	}
	defer c.Close()
	//
	targetClient := c
	if targetPool != nil {
		targetClient, err = targetPool.NewClient()
		if err != nil {
			return
		}
		defer targetClient.Close()
	}

	find0(c, xdb, func(listKey string, datas map[string]interface{}) (err error) {
		err = xdb.WriteToTarget(targetClient, xdb, listKey, datas)
		if err != nil {
			return
		}
		count++
		return
	})
	return
}
