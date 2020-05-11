for $ids in distinct-values(doc("purchaseorders.xml")//item/partid)
let $items:= doc("purchaseorders.xml")//item[partid=$ids]
let $price:= doc("products.xml")//product[@pid=$ids]/description/price
order by $ids
return
     <totalcost partid="{$ids}">
     {sum($items/quantity)*$price}
     </totalcost>