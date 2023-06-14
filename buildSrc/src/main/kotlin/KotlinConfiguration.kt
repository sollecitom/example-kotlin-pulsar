object KotlinConfiguration {

    object TargetJvm {

        const val version = "19"
    }

    object Compiler {

        val arguments = listOf("-opt-in=kotlin.Experimental")
        const val generateJavaParameters = true
    }
}