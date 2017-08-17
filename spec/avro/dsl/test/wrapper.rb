namespace 'test'

record :wrapped1 do
  required :i, :int
end

record :wrapped2 do
  required :i, :int
end

record :wrapper do
  required :sub1, :wrapped1
  required :sub2, :wrapped1
  required :sub3, :wrapped2
end
