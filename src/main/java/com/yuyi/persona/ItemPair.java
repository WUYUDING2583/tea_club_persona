package com.yuyi.persona;

import java.io.Serializable;

public class ItemPair implements Serializable {
    long a;
    long b;

    public ItemPair(long a, long b) {
        this.a = Math.min(a, b);
        this.b = Math.max(a, b);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        long result = 1;
        result = prime * result + a * 31;
        result = prime * result + b * 31;
        return (int) result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ItemPair other = (ItemPair) obj;
        if (other.a == a && other.b == b) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format("[%s, %s]", a, b);
    }
}
