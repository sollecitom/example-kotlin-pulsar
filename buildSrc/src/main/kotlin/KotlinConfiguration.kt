private val optIns = listOf("kotlin.Experimental", "kotlinx.coroutines.ExperimentalCoroutinesApi")
private val optInCompilerArguments = optIns.map { "-opt-in=$it" }

object KotlinConfiguration {

    object TargetJvm {

        const val version = "19"
    }

    object Compiler {

        val arguments = optInCompilerArguments
        const val generateJavaParameters = true
    }
}