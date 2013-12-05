
var uuid = require('node-uuid');

var emptyBuffer = new Buffer(0)

exports.make = function(pool){
	return {
		get: function(name, lastSequenceId, cb){//sourceId, cb){
			if(typeof(lastSequenceId) !== 'number') throw new Error('lastSequenceId missing or not a number: ' + lastSequenceId)
			//if(typeof(sourceId) !== 'string') throw new Error('sourceId missing or not a string: ' + sourceId)
			if(typeof(cb) !== 'function') throw new Error('cb missing or not a function: ' + cb)
			
			//console.log('getting after: ' + lastSequenceId)

			pool.execute('SELECT sequence_id,source_id,data FROM boxds_updates WHERE key = ? AND sequence_id > ?', [name, lastSequenceId], 1, function(err, result){
				try{
					if(err){
						console.log('ERROR: ' + err)
						cb(err)
					}else{
						var all = []
						if(result.rows.length === 0){
							cb(undefined, all)
						}else{
							var highest = result.rows[result.rows.length-1][0]
							for(var i=0;i<result.rows.length;++i){
								var row = result.rows[i]
								//if(row[1] === sourceId) continue
								all.push({sourceId: row[1], data: row[2]})//{sequenceId: row[0], data: row[2]})
							}
							cb(undefined, all, highest)
						}
					}
				}catch(e){
					console.log('ERROR: ' + e.stack)
				}
			})
		},
		put: function(name, updateBuf, sourceId, previousSequenceId, cb, failIfGreater){
			if(typeof(previousSequenceId) !== 'number') throw new Error('previousSequenceId missing or not a number: ' + previousSequenceId)
			
			var tryCount = 0
			var start = Date.now()
			//console.log('update put ' + name + ' ' + previousSequenceId + ' ' + failIfGreater)
			//console.log(new Error().stack)

			function trySequencing(sequenceId){
				//console.log('trying: ' + sequenceId)
				++tryCount
				pool.execute('INSERT INTO boxds_updates (key, sequence_id, source_id, data) VALUES (?, ?, ?, ?) IF NOT EXISTS', [name, sequenceId, sourceId, updateBuf], 1, function(err, result){
					if(err){
						console.log(require('util').inspect(err))
						throw err
					}
					try{
						//console.log(JSON.stringify(result))
						//retry with higher sequence id if necessary
						if(!result.rows[0][0]){
							//console.log('retrying: ' + (sequenceId+1))
							if(failIfGreater){
								if(tryCount > 5){
									console.log(JSON.stringify(result))
									throw new Error('failIfGreater: ' + sequenceId + ' ' + name + ' ' + tryCount)
								}else{
									console.log('retrying: ' + name + ' ' + sequenceId)
									trySequencing(sequenceId)
									return
								}
							}
							trySequencing(sequenceId+1)
						}else{
							console.log('done: ' + sequenceId + ' ' + tryCount + ' in ' + (Date.now() - start) + 'ms ' + name)
							if(cb) cb()
						}
					}catch(e){
						console.log('ERROR: ' + e.stack)
					}
				})
			}
			trySequencing(previousSequenceId+1)
		}
	}
}
