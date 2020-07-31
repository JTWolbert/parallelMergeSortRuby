module Merge

def Merge.merge_sort(node,cores,a)
        if node < cores
                rd1, wr1 = IO.pipe
                rd2, wr2 = IO.pipe
                mid = a.size/2
                pid1 = fork do
                        rd1.close
                        Marshal.dump(merge_sort(node * 2,cores,a.slice(0,mid)),wr1)
                        wr1.close
                end
                pid2 = fork do
                        rd2.close
                        Marshal.dump(merge_sort(node * 2 + 1,cores,a.slice(mid,a.count - mid)),wr2)
                        wr2.close
                end
                wr1.close
                wr2.close
                return combine(Marshal.load(rd1),Marshal.load(rd2))
                Process.wait(pid1)
                Process.wait(pid2)
                rd1.close
                rd2.close
        end
        if(a.size > 1)
                mid = a.size/2
                x=merge_sort(node * 2,cores,a.slice(0,mid))
                y=merge_sort(node * 2 + 1,cores,a.slice(mid,a.count - mid))
                return combine(x,y)
        end
        return a
end
def combine(a,b)
        i = 0
        j = 0
        c = Array.new
        while (i + j) < ( a.size + b.size )
                if i > a.size - 1
                c << b[j]
                j = j + 1
                elsif j > b.size - 1
                c << a[i]
                i = i + 1
                elsif a[i] > b[j]
                c << b[j]
                j = j + 1
                else
                c << a[i]
                i = i + 1
                end
        end
        
        return c
end
end
