package net.remware.k_sample

data class Alarm(val name: String,
                 val id: Int,
                 val source: String,
                 val sector: String,
                 val affected: String,
                 val category: String,
                 val severity: String
) {
    fun main() {
        println("this: $this")
    }
}

