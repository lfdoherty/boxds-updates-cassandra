
var uuid = require('node-uuid');

var emptyBuffer = new Buffer(0)

exports.make = function(pool){

	pool.execute('CREATE TABLE boxds_updates ('+
		'key varchar,'+
		'sequence_id int,'+
		'source_id timeuuid,'+
		'data blob,'+
		'PRIMARY KEY (key,sequence_id)'+
	');', function(){
	})


	return {
		get: function(name, lastSequenceId, cb){//sourceId, cb){
			if(typeof(lastSequenceId) !== 'number') throw new Error('lastSequenceId missing or not a number: ' + lastSequenceId)
			//if(typeof(sourceId) !== 'string') throw new Error('sourceId missing or not a string: ' + sourceId)
			if(typeof(cb) !== 'function') throw new Error('cb missing or not a function: ' + cb)
			

			pool.execute('SELECT sequence_id,source_id,data FROM boxds_updates WHERE key = ? AND sequence_id > ?', [name, lastSequenceId], 1, function(err, result){
				try{
					if(err){
						console.log('ERROR: ' + err)
						cb(err)
					}else{

						//console.log('got after: ' + lastSequenceId + ' ' + name + ' ' + result.rows.length)
						
						var all = []
						if(result.rows.length === 0){
							cb(undefined, all)
						}else{
							var highest = result.rows[result.rows.length-1].sequence_id
							for(var i=0;i<result.rows.length;++i){
								var row = result.rows[i]
								//if(row[1] === sourceId) continue
								all.push({sourceId: row.source_id, data: row.data})//{sequenceId: row[0], data: row[2]})
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
						
						//retry with higher sequence id if necessary
						if(!result.rows[0]['[applied]']){
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
							if(tryCount> 120){
								console.log(JSON.stringify(result))
								console.log('failed 120 times to insert update ' + sequenceId + ' ' + name)
								console.log('ERROR: ' + new Error().stack)
								return
							}
							trySequencing(sequenceId+1)
						}else{
							//console.log('done: ' + sequenceId + ' ' + tryCount + ' in ' + (Date.now() - start) + 'ms ' + name)
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
