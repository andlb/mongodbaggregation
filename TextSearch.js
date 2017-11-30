
it is going to search only the documents that contain all the text in any part of the document
db.modelos.aggregate([
  {"$match": { $text: { $search: "gol power" } }},    
  {"$match": { modelo: { $regex: /gol.*power.*flex.*/i, $nin: [ 'acmeblahcorp' ] } } } 
])